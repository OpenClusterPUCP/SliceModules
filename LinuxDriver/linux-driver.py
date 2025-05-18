# ==============================================================================
# | ARCHIVO: linux_driver.py
# ==============================================================================
# | DESCRIPCIÓN:
# | Módulo que implementa el backend para el control de vida de lices 
# | de red en un clúster Línux. Realiza el despliegue, configuración y gestión de VMs,
# | redes virtuales y recursos distribuidos a través de múltiples nodos workers.
# ==============================================================================
# | CONTENIDO PRINCIPAL:
# | 1. CONFIGURACIÓN INICIAL
# |    - Importaciones y configuración Flask/CORS/WebSocket
# |    - Estructura de directorios y archivos de datos
# |    - Configuración de nodos (workers y OFS)
# |    - Recursos disponibles (imágenes y flavors)
# |
# | 2. GESTORES/MÓDULOS PRINCIPALES
# |    - SSHManager: Conexiones SSH a nodos
# |    - ImageManager: Sincronización de imágenes
# |    - SliceManager: Gestión principal de slices
# |
# | 3. NETWORKING DISTRIBUIDO
# |    - Configuración de bridges OVS en workers
# |    - QinQ y VLANs para aislamiento
# |    - DHCP y acceso a internet centralizado
# |    - Patch ports entre bridges
# |    - Interfaces TAP para VMs
# |
# | 4. GESTIÓN DE VMs
# |    - Despliegue distribuido en workers
# |    - Ciclo de vida completo (crear/pausar/reanudar/reiniciar/eliminar)
# |    - Acceso VNC con websockets
# |    - Cleanup distribuido
# |
# | 5. API ENDPOINTS
# |    - /deploy-slice: Despliegue de nuevos slices
# |    - /stop-slice, /restart-slice: Control de slices
# |    - /pause-vm, /resume-vm, /restart-vm: Control de VMs
# |    - /sync-images: Sincronización de imágenes a todos los workers
# |
# | 6. UTILIDADES
# |    - Manejo de archivos JSON
# |    - Generación de IDs y configuraciones
# |    - Validación de recursos y topologías
# |    - Logging y manejo de errores
# |    - Cleanup y recuperación de errores
# ==============================================================================

# ===================== IMPORTACIONES =====================
from flask import Flask, request, jsonify, render_template
from py_eureka_client import eureka_client
from flask_cors import CORS
from flask_sock import Sock
import paramiko

# Networking y sistema:
import os
import sys
import time
import datetime
import subprocess
import traceback
import socket
import threading
import requests
from pathlib import Path


# Utilidades:
import json
import secrets
import select
from typing import Tuple, List, Dict

# Kafka y MySQL
from confluent_kafka import Consumer, KafkaError, KafkaException
import mysql.connector
import mysql.connector.pooling
from typing import List, Dict, Tuple
import threading
import uuid
import queue
import json
import time
from datetime import datetime

# ===================== CONFIGURACIÓN DE FLASK =====================
app = Flask(__name__, static_url_path='/static', static_folder='static')
sock = Sock(app)
CORS(app)

host = '0.0.0.0'
port = 5000
debug = True

# ===================== CONFIGURACIÓN DE EUREKA =====================
eureka_server = "http://10.0.10.1:8761"

eureka_client.init(
    eureka_server=eureka_server,
    app_name="linux-driver",
    instance_port=5000,
    instance_host="10.0.10.1",
    renewal_interval_in_secs=30,
    duration_in_secs=90,
)

def get_service_instance(service_name: str) -> dict:
    """
    Obtiene información de la instancia de un servicio registrado en Eureka.
    
    Args:
        service_name (str): Nombre del servicio registrado en Eureka
        
    Returns:
        dict: Información de la instancia con host y puerto, o None si no se encuentra
        
    Example:
        instance = get_service_instance('slice-manager')
        url = f"http://{instance['ipAddr']}:{instance['port']}/endpoint"
    """
    try:
        Logger.debug(f"Buscando instancia de servicio: {service_name}")
        
        # Obtener instancia a través del cliente Eureka
        instance = eureka_client.get_client().applications.get_application(service_name)
        if not instance or not instance.up_instances:
            Logger.error(f"Servicio {service_name} no encontrado en Eureka")
            return None
            
        # Obtener primera instancia disponible
        instance = instance.up_instances[0]
        
        service_info = {
            'ipAddr': instance.ipAddr,
            'port': instance.port.port,
            'hostName': instance.hostName
        }
        
        Logger.debug(f"Instancia encontrada: {json.dumps(service_info, indent=2)}")
        return service_info
        
    except Exception as e:
        Logger.error(f"Error obteniendo instancia de {service_name}: {str(e)}")
        return None
    
# ===================== CONSTANTES =====================
# Directorio de trabajo principal
WORKING_DIR = os.path.dirname(os.path.abspath(__file__))

# Estructura de directorios
DATA_DIR = {
    'root': os.path.join(WORKING_DIR, 'data'),
    'dhcp': {
        'config': os.path.join(WORKING_DIR, 'data', 'dhcp', 'config'),
        'log': os.path.join(WORKING_DIR, 'data', 'dhcp', 'log')
    },
    'images': os.path.join(WORKING_DIR, 'images'),
}

# Configuración de nodos
NODES = {
    'ofs': {
        'ip': '10.0.10.5',  
        'data_ip': '172.16.0.5',
        'key': 'keys/ofs',
        'switch': 'OFS'
    },
    'worker1': {
        'ip': '10.0.10.2',
        'data_ip': '172.16.0.2',
        'key': 'keys/worker1',
        'switch': 'br-int'
    },
    'worker2': {
        'ip': '10.0.10.3',
        'data_ip': '172.16.0.3',
        'key': 'keys/worker2',
        'switch': 'br-int'
    },
    'worker3': {
        'ip': '10.0.10.4',
        'data_ip': '172.16.0.4',
        'key': 'keys/worker3',
        'switch': 'br-int'
    }
}

# Configuración de red
NETWORK_CONFIG = {
    'dhcp_ns_name': 'ns-internet',
    'internet_interface': 'ens3',
    'headnode_br': 'br-int',
}

# ===================== LOCKS PARA SINCRONIZACIÓN =====================
# Lock global para operaciones de red (evita conflictos en configuración de bridges/namespace)
network_setup_lock = threading.RLock()

# Lock para operaciones de limpieza
cleanup_lock = threading.RLock()

# Lock para operaciones en OVS (evita comandos ovs-vsctl simultáneos)
ovs_lock = threading.RLock()

# ===================== CONFIGURACIÓN BD =====================
DB_CONFIG = {
    "host": "10.0.10.4",
    "user": "root",
    "password": "Branko",
    "port": 4000,
    "database": "cloud_v3"
}

POOL_CONFIG = {
    "pool_name": "cloudpool",
    "pool_size": 5,
    **DB_CONFIG
}

# ===================== CONFIGURACIÓN DE KAFKA =====================
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'linux-driver',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'max.poll.interval.ms': 300000,  # 5 minutos
    'session.timeout.ms': 30000,     # 30 segundos
    'heartbeat.interval.ms': 10000,  # 10 segundos
    #'max.poll.records': 1,           # Solo 1 mensaje por poll
    'isolation.level': 'read_committed'
}

# Configuración de tópicos por prioridad (cada zona tendrá 3 tópicos)
KAFKA_TOPICS = {
    'HIGH': {
        'name': 'linux-zone1-high',
        'partitions': 8,
        'worker_threads': 8
    },
    'MEDIUM': {
        'name': 'linux-zone1-medium',
        'partitions': 4,
        'worker_threads': 4
    },
    'LOW': {
        'name': 'linux-zone1-low',
        'partitions': 2,
        'worker_threads': 2
    }
}

# Cola compartida de operaciones por SliceId para mantener orden FIFO
slice_operation_queues = {}
slice_operation_locks = {}
slice_queue_lock = threading.RLock()

# Estado global de consumidores
consumers_running = True


# ===================== UTILIDADES =====================
# Funciones auxiliares para manejo de archivos, directorios y logs

def init_directories() -> bool:
    """
    Crea la estructura de directorios necesaria para el funcionamiento del sistema.
    
    Esta función inicializa todos los directorios definidos en DATA_DIR, incluyendo:
    - Directorio raíz de datos
    - Directorios para slices
    - Directorios para configuración de red
    - Directorios DHCP (configuración y logs)
    - Directorio de imágenes
    - Directorio de pruebas

    Returns:
        bool: True si todos los directorios se crearon correctamente, False en caso de error
    """
    try:
        Logger.section("INICIALIZANDO ESTRUCTURA DE DIRECTORIOS")
        
        for dir_name, dir_path in DATA_DIR.items():
            if isinstance(dir_path, dict):
                Logger.info(f"Creando subdirectorios para {dir_name}...")
                for sub_name, sub_path in dir_path.items():
                    Logger.debug(f"Creando {sub_name}: {sub_path}")
                    os.makedirs(sub_path, exist_ok=True)
            else:
                Logger.info(f"Creando directorio {dir_name}: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)

        Logger.success("Estructura de directorios creada exitosamente")
        return True
        
    except Exception as e:
        Logger.error(f"Error creando estructura de directorios: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return False

def load_json_file(file_path: str, default_content: dict = None) -> dict:
    """
    Carga un archivo JSON o crea uno nuevo con contenido por defecto si no existe.
    
    Args:
        file_path: Ruta del archivo JSON a cargar
        default_content: Diccionario con contenido por defecto si el archivo no existe

    Returns:
        dict: Contenido del archivo JSON o default_content si hay error
    """
    try:
        Logger.debug(f"Cargando archivo JSON: {file_path}")
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = json.load(f)
                Logger.debug(f"Archivo cargado exitosamente")
                return content
        else:
            Logger.warning(f"Archivo no existe, creando con contenido por defecto")
            content = default_content or {}
            save_json_file(file_path, content)
            return content
            
    except Exception as e:
        Logger.error(f"Error cargando archivo JSON {file_path}: {str(e)}")
        Logger.debug(f"Usando contenido por defecto")
        return default_content or {}

def save_json_file(file_path: str, content: dict) -> bool:
    """
    Guarda contenido en un archivo JSON, creando directorios si es necesario.
    
    Args:
        file_path: Ruta donde guardar el archivo JSON
        content: Diccionario con contenido a guardar

    Returns:
        bool: True si se guardó correctamente, False en caso de error
    """
    try:
        Logger.debug(f"Guardando archivo JSON: {file_path}")
        
        # Crear directorio padre si no existe
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Guardar archivo con formato legible
        with open(file_path, 'w') as f:
            json.dump(content, f, indent=2)
            
        Logger.debug(f"Archivo guardado exitosamente")
        return True
        
    except Exception as e:
        Logger.error(f"Error guardando archivo JSON {file_path}: {str(e)}")
        Logger.debug(f"Contenido que se intentó guardar: {json.dumps(content, indent=2)}")
        return False

class Logger:
    """Clase para manejar el formato y presentación de logs del sistema"""

    # Colores ANSI
    HEADER = '\033[95m'    # Morado
    BLUE = '\033[94m'      # Azul
    GREEN = '\033[92m'     # Verde
    YELLOW = '\033[93m'    # Amarillo
    RED = '\033[91m'       # Rojo
    CYAN = '\033[96m'      # Cyan
    WHITE = '\033[97m'     # Blanco brillante
    ENDC = '\033[0m'       # Reset color
    BOLD = '\033[1m'       # Negrita
    DIM = '\033[2m'        # Tenue

    @staticmethod
    def _get_timestamp():
        """Retorna el timestamp actual en formato legible"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def major_section(title):
        """Imprime una sección principal con timestamp y separadores grandes"""
        timestamp = Logger._get_timestamp()
        print(f"\n{Logger.HEADER}{Logger.BOLD}")
        print("═" * 100)
        print(f"║ {Logger.CYAN}{timestamp}{Logger.HEADER} ║ {Logger.WHITE}{title}")
        print("═" * 100)
        print(f"{Logger.ENDC}")

    @staticmethod
    def section(title):
        """Imprime una sección con separadores medianos"""
        timestamp = Logger._get_timestamp()
        print(f"\n{Logger.BLUE}{Logger.BOLD}")
        print("─" * 100)
        print(f"┌─[{Logger.CYAN}{timestamp}{Logger.BLUE}]")
        print(f"└─▶ {Logger.WHITE}{title}")
        print("─" * 100)
        print(f"{Logger.ENDC}")

    @staticmethod
    def subsection(title):
        """Imprime una subsección con separadores pequeños"""
        timestamp = Logger._get_timestamp()
        print(f"\n{Logger.BLUE}{Logger.BOLD}")
        print("·" * 80)
        print(f"▶ [{Logger.DIM}{timestamp}{Logger.BLUE}] {Logger.WHITE}{title}")
        print("·" * 80)
        print(f"{Logger.ENDC}")

    @staticmethod
    def info(message):
        """Imprime mensaje informativo con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.GREEN}[{Logger.DIM}{timestamp}{Logger.GREEN}] ℹ {message}{Logger.ENDC}")

    @staticmethod
    def warning(message):
        """Imprime advertencia con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.YELLOW}[{Logger.DIM}{timestamp}{Logger.YELLOW}] ⚠ {message}{Logger.ENDC}")

    @staticmethod
    def error(message):
        """Imprime error con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.RED}[{Logger.DIM}{timestamp}{Logger.RED}] ✗ {message}{Logger.ENDC}")

    @staticmethod
    def debug(message):
        """Imprime mensaje de debug con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.BLUE}[{Logger.DIM}{timestamp}{Logger.BLUE}] 🔍 {message}{Logger.ENDC}")

    @staticmethod
    def success(message):
        """Imprime mensaje de éxito con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.GREEN}[{Logger.DIM}{timestamp}{Logger.GREEN}] ✓ {message}{Logger.ENDC}")

    @staticmethod
    def failed(message):
        """Imprime mensaje de fallo con timestamp"""
        timestamp = Logger._get_timestamp()
        print(f"{Logger.RED}[{Logger.DIM}{timestamp}{Logger.RED}] ⨯ {message}{Logger.ENDC}")


# ===================== SUBMÓDULOS =====================
# Clases y funciones que permiten controlar el ciclo de vida de las VMs y slices en Linux Clúster

class SSHManager:
    """
    Administrador de conexiones SSH para comunicación con workers.
    
    Esta clase maneja la conexión SSH a los workers del clúster, permitiendo:
    - Establecer conexiones SSH seguras usando claves o contraseñas
    - Ejecutar comandos remotos y obtener su salida
    - Manejar errores de conexión y ejecución de comandos
    - Gestionar recursos mediante context manager (with)
    
    Attributes:
        hostname (str): Nombre del host remoto
        ip (str): Dirección IP del host remoto
        username (str): Usuario SSH
        password (str): Contraseña SSH (opcional)
        key_path (str): Ruta a la clave privada SSH
        client (paramiko.SSHClient): Cliente SSH de paramiko
    """

    def __init__(self, worker_info: dict):
        """
        Inicializa el administrador SSH con la información del worker.

        Args:
            worker_info (dict): Diccionario con la información de conexión:
                - name/hostname: Nombre del host
                - ip: Dirección IP
                - ssh_username: Usuario SSH
                - ssh_password: Contraseña (opcional)
                - ssh_key_path: Ruta a la clave privada

        Raises:
            ValueError: Si falta información requerida (IP o username)
        """
        Logger.debug("Iniciando SSHManager")
        Logger.debug(f"Información del worker recibida: {json.dumps(worker_info, indent=2)}")
        
        # Extraer y validar campos requeridos
        self.hostname = worker_info.get('name') or worker_info.get('hostname')
        self.ip = worker_info.get('ip')
        self.username = worker_info.get('ssh_username')
        self.password = worker_info.get('ssh_password')
        self.key_path = worker_info.get('ssh_key_path')
        
        # Validar campos obligatorios
        if not all([self.ip, self.username]):
            Logger.error("Falta información requerida para conexión SSH")
            raise ValueError("Falta IP o username para conexión SSH")
    
        self.client = None

    def __enter__(self):
        """
        Establece la conexión SSH al entrar en el contexto 'with'.
        
        Returns:
            SSHManager: La instancia actual con la conexión establecida

        Raises:
            Exception: Si hay error en la conexión SSH
        """
        Logger.subsection(f"Estableciendo conexión SSH con {self.hostname} ({self.ip})")
        
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            Logger.info(f"Conectando a {self.ip} como {self.username}")
            self.client.connect(
                hostname=self.ip,
                username=self.username,
                key_filename=self.key_path
            )
            Logger.success("Conexión SSH establecida exitosamente")
            return self
            
        except Exception as e:
            Logger.error(f"Error en conexión SSH: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra la conexión SSH al salir del contexto 'with'"""
        if self.client:
            self.client.close()
            Logger.debug(f"Conexión SSH con {self.ip} cerrada")

    def execute(self, command: str) -> tuple:
        """
        Ejecuta un comando SSH y retorna su salida.
        
        Args:
            command (str): Comando a ejecutar en el host remoto

        Returns:
            tuple: (stdout, stderr) con la salida del comando

        Raises:
            Exception: Si hay error en la ejecución del comando
        """
        try:
            Logger.debug(f"Ejecutando comando en {self.hostname} ({self.ip}):")
            Logger.info(f"$ {command}")
            
            # Ejecutar comando
            stdin, stdout, stderr = self.client.exec_command(command)
            
            # Leer salida y estado
            stdout_str = stdout.read().decode().strip()
            stderr_str = stderr.read().decode().strip()
            exit_status = stdout.channel.recv_exit_status()
            
            # Registrar resultado
            Logger.debug(f"Estado de salida: {exit_status}")
            if stdout_str:
                Logger.debug(f"Salida estándar:\n{stdout_str}")
            if stderr_str:
                Logger.debug(f"Salida de error:\n{stderr_str}")
            
            # Manejar errores
            if exit_status != 0:
                error_msg = [f"Comando SSH falló (status={exit_status})"]
                if stderr_str:
                    error_msg.append(f"Error: {stderr_str}")
                if stdout_str:
                    error_msg.append(f"Output: {stdout_str}")
                raise Exception('\n'.join(error_msg))
                
            return stdout_str, stderr_str
            
        except paramiko.SSHException as e:
            error_msg = f"Error de conexión SSH a {self.ip}: {str(e)}"
            Logger.error(error_msg)
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(error_msg)
            
        except Exception as e:
            if isinstance(e, Exception) and "Comando SSH falló" in str(e):
                Logger.error(str(e))
                raise
            error_msg = f"Error ejecutando comando en {self.ip}: {str(e)}"
            Logger.error(error_msg)
            Logger.debug(f"Comando que falló: {command}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(error_msg)

class SliceManager:
    """
    Administrador principal de slices y recursos distribuidos del sistema.

    Esta clase se encarga de:
    - Gestionar el ciclo de vida completo de los slices
    - Coordinar recursos distribuidos entre workers
    - Manejar configuración de red y VNC
    - Administrar despliegue y control de VMs

    Attributes:
        worker_assigner (WorkerAssigner): Asignador de VMs a workers
        image_manager (ImageManager): Gestor de imágenes entre nodos
    """

    def __init__(self):
        """Inicializa las dependencias necesarias para gestión de slices."""
        self.image_manager = ImageManager()
        Logger.debug("SliceManager inicializado con sus dependencias")

    def deploy_slice(self, slice_config: dict) -> dict:
        """
        Despliega un nuevo slice usando la configuración preprocesada.
        
        Coordina todo el proceso de despliegue incluyendo:
        1. Sincronización de imágenes entre nodos
        2. Configuración de red y DHCP
        3. Configuración paralela de workers
        4. Despliegue de VMs
        
        Args:
            slice_config (dict): Configuración completa del slice a desplegar
            
        Returns:
            dict: Configuración actualizada con información de despliegue
            
        Raises:
            Exception: Si hay errores en cualquier etapa del despliegue
        """
        current_stage = None
        worker_threads = []
        worker_results = {}
        worker_errors = {}

        try:
            Logger.major_section("INICIANDO DESPLIEGUE DE SLICE")
            Logger.info(f"Slice ID: {slice_config['slice_info']['id']}")

            # 1. Verificar y sincronizar imágenes
            current_stage = 'images'
            Logger.section("FASE 1: SINCRONIZACIÓN DE IMÁGENES")
            image_manager = ImageManager()
            
            # Debug: Imprimir configuración recibida
            Logger.debug("Configuración recibida:")
            Logger.debug(f"Topology info: {json.dumps(slice_config.get('topology_info', {}), indent=2)}")
            Logger.debug(f"Resources info: {json.dumps(slice_config.get('resources_info', {}), indent=2)}")
            # Agrupar VMs por worker e imagen
            vms_by_worker = {}
            try:
                for vm in slice_config['topology_info']['vms']:
                    Logger.debug(f"\nProcesando VM: ID-{vm['id']}")
                    worker_id = str(vm['physical_server']['id'])
                    Logger.debug(f"Worker ID: {worker_id}")
                    
                    # Verificar que existe la información del worker
                    if worker_id not in slice_config['resources_info']['workers']:
                        raise Exception(f"No se encontró información del worker {worker_id}")
                        
                    worker_info = slice_config['resources_info']['workers'][worker_id]
                    Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")
                    
                    # Verificar que existe la información de la imagen
                    image_id = str(vm['image_id'])  # Convertir a string
                    if image_id not in slice_config['resources_info']['images']:
                        raise Exception(f"No se encontró información de la imagen {image_id}")
                        
                    image_info = slice_config['resources_info']['images'][image_id]
                    Logger.debug(f"Image info: {json.dumps(image_info, indent=2)}")
                    
                    worker_name = worker_info['name']
                    if worker_name not in vms_by_worker:
                        vms_by_worker[worker_name] = {
                            'info': worker_info,
                            'vms': [],
                            'images': set()
                        }
                    vms_by_worker[worker_name]['vms'].append(vm)
                    vms_by_worker[worker_name]['images'].add(image_info['path'])
                    
            except KeyError as e:
                raise Exception(f"Falta información requerida en la configuración: {str(e)}")
            except Exception as e:
                raise Exception(f"Error procesando VMs: {str(e)}")

            # Debug: Imprimir agrupación resultante
            Logger.debug("VMs agrupadas por worker:")
            for worker_name, data in vms_by_worker.items():
                Logger.debug(f"\nWorker: {worker_name}")
                Logger.debug(f"VMs: {[vm['id'] for vm in data['vms']]}")
                Logger.debug(f"Images: {data['images']}")

            # Sincronizar imágenes necesarias
            Logger.info("Verificando y sincronizando imágenes en workers...")
            for worker_name, worker_data in vms_by_worker.items():
                Logger.subsection(f"Procesando worker {worker_name}")
                for image_path in worker_data['images']:
                    try:
                        Logger.info(f"Verificando imagen {os.path.basename(image_path)}...")
                        if not os.path.exists(image_path):
                            raise Exception(f"Imagen no encontrada en el headnode: {image_path}")
                            
                        if not image_manager.check_image_in_worker(worker_data['info'], image_path):
                            Logger.info(f"Sincronizando {os.path.basename(image_path)} a {worker_name}...")
                            if not image_manager.sync_image_to_worker(worker_data['info'], image_path):
                                raise Exception(f"Error sincronizando imagen")
                    except Exception as e:
                        raise Exception(f"Error con imagen {os.path.basename(image_path)} en {worker_name}: {str(e)}")


            # 2. Configurar acceso a internet y DHCP
            current_stage = 'network'
            Logger.section("FASE 2: CONFIGURACIÓN DE RED")

            # Inyectar slice_id en network_config para nombres únicos (AJUSTE PARA PARALELISMO)
            slice_config['network_config']['slice_id'] = slice_config['slice_info']['id']
            self._setup_internet_access(slice_config['network_config'])

            # 3. Configuración paralela de workers
            current_stage = 'worker_setup'
            Logger.section("FASE 3: CONFIGURACIÓN DE WORKERS")

            for worker_name, worker_data in vms_by_worker.items():
                thread = threading.Thread(
                    target=self._worker_setup,
                    args=(worker_data['info'], worker_data['vms'], 
                        slice_config, worker_results, worker_errors)
                )
                worker_threads.append(thread)
                thread.start()
                Logger.debug(f"Thread iniciado para worker {worker_name}")

            # Esperar threads
            Logger.info("Esperando finalización de workers...")
            for thread in worker_threads:
                thread.join()

            # Verificar errores
            if worker_errors:
                error_messages = [f"{worker}: {error}" 
                                for worker, error in worker_errors.items()]
                raise Exception("Errores en workers:\n" + "\n".join(error_messages))

            # Actualizar estado
            Logger.section("ACTUALIZANDO ESTADO FINAL")
            for worker_name, result in worker_results.items():
                if result.get('vms'):
                    for vm_data in result['vms']:
                        for vm in slice_config['topology_info']['vms']:
                            if vm['id'] == vm_data['id']:
                                vm.update({
                                    'qemu_pid': vm_data.get('qemu_pid'),
                                    'status': 'running' if vm_data.get('qemu_pid') else 'error'
                                })
                                Logger.debug(f"VM ID-{vm['id']} actualizada: "
                                        f"PID={vm_data.get('qemu_pid')}, "
                                        f"status={'running' if vm_data.get('qemu_pid') else 'error'}")

            slice_config['slice_info']['status'] = 'running'
            Logger.success("Slice desplegado exitosamente")
            return slice_config

        except Exception as e:
            error_msg = f"Error en etapa '{current_stage}': {str(e)}"
            Logger.error(error_msg)
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            Logger.section("INICIANDO LIMPIEZA POR ERROR")
            self._cleanup_deployment_state(slice_config, current_stage)
            raise Exception(error_msg)

    # def _worker_setup(self, worker_info: dict, vms: list, slice_config: dict, 
    #                 results: dict, errors: dict):
    #     """
    #     Configura un worker específico para el despliegue de sus VMs asignadas.
        
    #     Realiza la configuración completa de un worker incluyendo:
    #     1. Configuración de bridges OVS
    #     2. Configuración de interfaces TAP
    #     3. Preparación de imágenes
    #     4. Inicio de VMs
        
    #     Args:
    #         worker_info (dict): Información del worker a configurar
    #         vms (list): Lista de VMs a desplegar en este worker
    #         slice_config (dict): Configuración completa del slice
    #         results (dict): Diccionario para almacenar resultados
    #         errors (dict): Diccionario para almacenar errores
    #     """
    #     try:
    #         Logger.subsection(f"CONFIGURANDO WORKER ID-{worker_info['id']}")
    #         Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")
    #         Logger.info(f"VMs a configurar: {len(vms)}")

    #         # Preparar información SSH
    #         ssh_info = {
    #             'name': worker_info['name'],
    #             'ip': worker_info['ip'],
    #             'ssh_username': worker_info['ssh_username'],
    #             'ssh_password': worker_info.get('ssh_password'),
    #             'ssh_key_path': worker_info.get('ssh_key_path')
    #         }

    #         with SSHManager(ssh_info) as ssh:
    #             # 1. Configuración de bridges
    #             Logger.info(f"Configurando bridges en {worker_info['name']}...")
    #             bridge_commands = self._generate_bridge_commands(slice_config['network_config'])
    #             Logger.debug(f"Comandos bridge: {' && '.join(bridge_commands)}")
    #             ssh.execute(" && ".join(bridge_commands))
                
    #             # 2. Configuración de interfaces TAP
    #             Logger.info(f"Configurando interfaces TAP en {worker_info['name']}...")
    #             tap_commands = self._generate_tap_commands(vms, slice_config)
                
    #             if tap_commands:
    #                 Logger.debug(f"Ejecutando comandos TAP: {' && '.join(tap_commands)}")
    #                 ssh.execute(" && ".join(tap_commands))


    #             #Logger.info(f"Preparando imágenes en {worker_info['name']}...")
    #             # image_commands = self._generate_image_commands(vms, slice_config)
    #             #if image_commands:
    #             #    Logger.debug(f"Comandos imagen: {' && '.join(image_commands)}")
    #             #    ssh.execute(" && ".join(image_commands))

    #             # 3. Iniciar VMs
    #             vm_results = []
    #             for vm in vms:
    #                 Logger.info(f"Iniciando VM ID-{vm['id']} en {worker_info['name']}...")
    #                 self._start_single_vm(ssh, vm, slice_config)
                    
    #                 # Verificar estado
    #                 stdout, _ = ssh.execute(
    #                     f"pgrep -fa 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'"
    #                 )
    #                 if stdout.strip():
    #                     qemu_pid = int(stdout.split()[0])
    #                     Logger.success(f"VM ID-{vm['id']} iniciada con PID {qemu_pid}")
    #                     vm_results.append({
    #                         'id': vm['id'],
    #                         'qemu_pid': qemu_pid,
    #                         'status': 'running'
    #                     })

    #             results[worker_info['name']] = {
    #                 'success': True,
    #                 'vms': vm_results
    #             }
    #             Logger.success(f"Configuración de {worker_info['name']} completada")
                    
    #     except Exception as e:
    #         error_msg = f"Error en worker {worker_info['name']}: {str(e)}"
    #         Logger.error(error_msg)
    #         Logger.debug(f"Traceback: {traceback.format_exc()}")
    #         errors[worker_info['name']] = str(e)
    #         results[worker_info['name']] = {
    #             'success': False,
    #             'error': str(e)
    #         }

    def _worker_setup(self, worker_info: dict, vms: list, slice_config: dict, 
                    results: dict, errors: dict):
        """
        Configura un worker específico para el despliegue de sus VMs asignadas.
        
        La configuración se realiza en dos fases:
        1. Serie: Configuración de red (bridges y TAPs)
        2. Paralelo: Inicio de VMs usando múltiples conexiones SSH
        """
        try:
            Logger.subsection(f"CONFIGURANDO WORKER ID-{worker_info['id']}")
            Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")
            Logger.info(f"VMs a configurar: {len(vms)}")

            ssh_info = {
                'name': worker_info['name'],
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            # FASE 1: Configuración de red (en serie)
            with SSHManager(ssh_info) as ssh:
                # 1. Configuración de bridges
                Logger.info(f"Configurando bridges en {worker_info['name']}...")
                bridge_commands = self._generate_bridge_commands(slice_config['network_config'])
                Logger.debug(f"Comandos bridge: {' && '.join(bridge_commands)}")
                ssh.execute(" && ".join(bridge_commands))
                
                # 2. Configuración de interfaces TAP
                Logger.info(f"Configurando interfaces TAP en {worker_info['name']}...")
                tap_commands = self._generate_tap_commands(vms, slice_config)
                if tap_commands:
                    Logger.debug(f"Ejecutando comandos TAP: {' && '.join(tap_commands)}")
                    ssh.execute(" && ".join(tap_commands))

            # FASE 2: Inicio paralelo de VMs
            vm_threads = []
            vm_results_dict = {}
            vm_errors_dict = {}
            thread_lock = threading.Lock()

            def start_vm_thread(vm):
                try:
                    with SSHManager(ssh_info) as vm_ssh:
                        Logger.info(f"Iniciando VM ID-{vm['id']} en {worker_info['name']}...")
                        self._start_single_vm(vm_ssh, vm, slice_config)
                        
                        # Verificar estado
                        stdout, _ = vm_ssh.execute(
                            f"pgrep -fa 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'"
                        )
                        if stdout.strip():
                            qemu_pid = int(stdout.split()[0])
                            Logger.success(f"VM ID-{vm['id']} iniciada con PID {qemu_pid}")
                            with thread_lock:
                                vm_results_dict[vm['id']] = {
                                    'id': vm['id'],
                                    'qemu_pid': qemu_pid,
                                    'status': 'running'
                                }
                except Exception as e:
                    error_msg = f"Error iniciando VM ID-{vm['id']}: {str(e)}"
                    Logger.error(error_msg)
                    with thread_lock:
                        vm_errors_dict[vm['id']] = error_msg

            # Crear y comenzar threads para cada VM
            for vm in vms:
                thread = threading.Thread(target=start_vm_thread, args=(vm,))
                vm_threads.append(thread)
                thread.start()

            # Esperar a que todos los threads terminen
            for thread in vm_threads:
                thread.join()

            # Procesar resultados
            if vm_errors_dict:
                error_messages = [f"VM ID-{vm_id}: {error}" 
                                for vm_id, error in vm_errors_dict.items()]
                raise Exception("Errores iniciando VMs:\n" + "\n".join(error_messages))

            results[worker_info['name']] = {
                'success': True,
                'vms': list(vm_results_dict.values())
            }
            Logger.success(f"Configuración de {worker_info['name']} completada")

        except Exception as e:
            error_msg = f"Error en worker {worker_info['name']}: {str(e)}"
            Logger.error(error_msg)
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            errors[worker_info['name']] = str(e)
            results[worker_info['name']] = {
                'success': False,
                'error': str(e)
            }

    def _generate_bridge_commands(self, network_config: dict) -> list:
        """
        Genera comandos OVS para configurar los bridges y patch ports necesarios.

        Genera la secuencia de comandos para:
        1. Limpiar configuración existente (puertos y bridges)
        2. Crear nuevo bridge para el slice
        3. Configurar patch ports entre bridges
        4. Configurar QinQ y VLANs

        Args:
            network_config (dict): Configuración de red del slice con:
                - slice_bridge_name: Nombre del bridge del slice
                - patch_ports: Configuración de patch ports
                - svlan_id: ID del SVLAN para QinQ

        Returns:
            list: Lista de comandos OVS a ejecutar
        """
        Logger.debug("Generando comandos de configuración de bridges")
        
        slice_bridge = network_config['slice_bridge_name']
        patch_slice = network_config['patch_ports']['slice_side']
        patch_int = network_config['patch_ports']['int_side']
        svlan = network_config['svlan_id']
        
        Logger.debug(f"Bridge del slice: {slice_bridge}")
        Logger.debug(f"Patch ports: {patch_slice} <-> {patch_int}")
        Logger.debug(f"SVLAN: {svlan}")
        
        return [
            # Limpiar configuración existente
            f"sudo ovs-vsctl --if-exists del-port br-int {patch_int}",
            f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {patch_slice}",
            f"sudo ovs-vsctl --if-exists del-br {slice_bridge}",
            # Crear nuevo bridge y configurar
            f"sudo ovs-vsctl add-br {slice_bridge}",
            f"sudo ovs-vsctl add-port {slice_bridge} {patch_slice} -- set interface {patch_slice} type=patch options:peer={patch_int}",
            f"sudo ovs-vsctl add-port br-int {patch_int} -- set interface {patch_int} type=patch options:peer={patch_slice} -- set port {patch_int} vlan_mode=dot1q-tunnel tag={svlan}"
        ]

    def _generate_tap_commands(self, vms: list, slice_config: dict) -> list:
        """
        Genera comandos para crear y configurar interfaces TAP de las VMs.

        Genera comandos para:
        1. Limpiar interfaces TAP existentes
        2. Crear nuevas interfaces TAP
        3. Configurar VLANs según tipo de acceso:
        - Externo: SVLAN en br-int
        - Interno: CVLAN en bridge del slice

        Args:
            vms (list): Lista de VMs a configurar
            slice_config (dict): Configuración completa del slice

        Returns:
            list: Lista de comandos para configurar interfaces TAP
        """
        Logger.debug("Generando comandos de configuración TAP")
        
        commands = []
        network_config = slice_config['network_config']
        slice_bridge = network_config['slice_bridge_name']
        
        for vm in vms:
            Logger.debug(f"Procesando interfaces de VM ID-{vm['id']}")
            vm_interfaces = [i for i in slice_config['topology_info']['interfaces']
                            if i['vm_id'] == vm['id']]
            
            for interface in vm_interfaces:
                tap_name = interface.get('tap_name')
                if not tap_name:
                    Logger.warning(f"Interface sin tap_name para VM ID-{vm['id']}")
                    continue
                
                Logger.debug(f"Configurando TAP {tap_name}")
                
                # Limpieza y creación de TAP
                commands.extend([
                    f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {tap_name}",
                    f"sudo ovs-vsctl --if-exists del-port br-int {tap_name}",
                    f"sudo ip link set {tap_name} down 2>/dev/null || true",
                    f"sudo ip tuntap del {tap_name} mode tap 2>/dev/null || true",
                    f"sudo ip tuntap add mode tap name {tap_name}",
                    f"sudo ip link set {tap_name} up"
                ])
                
                # Configuración según tipo de interfaz
                if interface['external_access']:
                    Logger.debug(f"Interface externa: {tap_name} -> br-int")
                    commands.extend([
                        f"sudo ovs-vsctl add-port br-int {tap_name}",
                        f"sudo ovs-vsctl set port {tap_name} tag={network_config['svlan_id']}",
                        f"sudo ovs-vsctl set port {tap_name} vlan_mode=access"
                    ])
                else:
                    Logger.debug(f"Interface interna: {tap_name} -> {slice_bridge}")
                    link = next(
                        (l for l in slice_config['topology_info']['links']
                        if l['id'] == interface['link_id']),
                        None
                    )
                    if link:
                        commands.extend([
                            f"sudo ovs-vsctl add-port {slice_bridge} {tap_name}",
                            f"sudo ovs-vsctl set port {tap_name} tag={link['cvlan_id']}"
                        ])
                    else:
                        Logger.warning(f"No se encontró link para interface {tap_name}")
        
        return commands

    def _start_single_vm(self, ssh, vm: dict, slice_config: dict):
        """
        Inicia una VM individual en un worker usando QEMU/KVM.

        Realiza los siguientes pasos:
        1. Verifica imagen y recursos asignados
        2. Configura permisos necesarios
        3. Construye comando QEMU con:
        - Recursos (RAM, CPUs)
        - Imagen y formato
        - Interfaces de red
        - VNC y opciones de logging
        4. Ejecuta y verifica estado de la VM

        Args:
            ssh (SSHManager): Conexión SSH al worker
            vm (dict): Configuración de la VM a iniciar
            slice_config (dict): Configuración completa del slice

        Raises:
            Exception: Si hay error iniciando la VM o QEMU no está corriendo
        """
        try:
            Logger.subsection(f"INICIANDO VM ID-{vm['id']}")
            
            # Convertir IDs a string para acceder al diccionario
            flavor_id = str(vm['flavor_id'])
            image_id = str(vm['image_id'])
            
            # Obtener configuración de recursos desde resources_info
            flavor_info = slice_config['resources_info']['flavors'][flavor_id]
            image_info = slice_config['resources_info']['images'][image_id]
            
            Logger.debug(f"Flavor: {json.dumps(flavor_info, indent=2)}")
            Logger.debug(f"Image: {json.dumps(image_info, indent=2)}")
            
            # Construir rutas de imagen
            base_image = os.path.basename(image_info['path'])
            vm_image = f"vm-{str(vm['id'])}-slice-{str(slice_config['slice_info']['id'])}.qcow2"
            base_path = os.path.join('/home/ubuntu/SliceManager/base', base_image)
            vm_image_path = os.path.join('/home/ubuntu/SliceManager/images', vm_image)

            # Preparar imagen de VM
            Logger.info("Preparando imagen de VM...")
            Logger.debug(f"Imagen base: {base_image}")
            Logger.debug(f"Imagen VM: {vm_image}")
            Logger.debug(f"Tamaño disco: {flavor_info['disk']}G")
            
            # Calcular tamaño exacto en bytes para 2GB
            desired_size_gb = flavor_info['disk']
            size_bytes = int(desired_size_gb * (1024 * 1024 * 1024))
            adjusted_size_bytes = ((size_bytes + 511) // 512) * 512

            # Convertir a MB para qemu-img
            adjusted_size_mb = adjusted_size_bytes // (1024 * 1024)

            # Copiar y redimensionar imagen usando los MB
            ssh.execute(f"sudo cp {base_path} {vm_image_path}")
            ssh.execute(f"sudo qemu-img resize {vm_image_path} {adjusted_size_mb}M")
            ssh.execute(f"sudo chmod 644 {vm_image_path}")

            # Verificar el tamaño final
            stdout, _ = ssh.execute(f"qemu-img info {vm_image_path}")
            Logger.debug(f"Información de imagen después del resize:\n{stdout}")

            # Verificar el tamaño final
            stdout, _ = ssh.execute(f"qemu-img info {vm_image_path}")
            Logger.debug(f"Información de imagen después del resize:\n{stdout}")

            # Verificar imagen creada
            Logger.info("Verificando imagen creada...")
            stdout, _ = ssh.execute(f"ls -lh {vm_image_path}")
            Logger.debug(f"Imagen: {stdout}")
            stdout, _ = ssh.execute(f"qemu-img info {vm_image_path}")
            Logger.debug(f"Info QEMU:\n{stdout}")
            
            # Verificar permisos
            Logger.info("Verificando permisos...")
            ssh.execute(f"sudo chmod 644 {vm_image_path}")
            
            # Construir comando QEMU
            cmd = [
                "sudo qemu-system-x86_64",
                "-enable-kvm", 
                f"-name guest=VM{vm['id']}-S{slice_config['slice_info']['id']}",
                f"-m {flavor_info['ram']}",
                f"-smp {flavor_info['vcpus']}",
                f"-drive file={vm_image_path},format=qcow2",
                f"-vnc 0.0.0.0:{vm['vnc_display']}",
                "-daemonize",
                "-D /tmp/qemu.log",
                "-d guest_errors"
            ]
            
            # Obtener y ordenar interfaces
            vm_interfaces = [
                i for i in slice_config['topology_info']['interfaces']
                if i['vm_id'] == vm['id']
            ]
            # Ordenar: primero las interfaces con acceso externo
            vm_interfaces.sort(key=lambda x: not x['external_access'])
            
            # Agregar interfaces en orden
            Logger.info("Configurando interfaces...")
            for interface in vm_interfaces:
                tap_name = interface['tap_name']  # Usar nombre TAP proporcionado
                cmd.extend([
                    f"-netdev tap,id={tap_name},ifname={tap_name},script=no,downscript=no",
                    f"-device e1000,netdev={tap_name},mac={interface['mac_address']}"
                ])
                Logger.debug(f"Interface: {tap_name}, MAC: {interface['mac_address']}")
            
            # Ejecutar QEMU y capturar salida
            Logger.info("Ejecutando QEMU...")
            Logger.debug(f"Comando: {' '.join(cmd)}")
            stdout, stderr = ssh.execute(' '.join(cmd))
            
            if stderr:
                Logger.warning(f"QEMU stderr: {stderr}")
                
            # Verificar si QEMU está corriendo
            Logger.info("Verificando estado...")
            time.sleep(2)
            stdout, _ = ssh.execute(f"pgrep -fa 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'")
            if not stdout.strip():
                stdout, _ = ssh.execute("tail -n 50 /tmp/qemu.log")
                raise Exception(f"QEMU no está corriendo. Logs:\n{stdout}")
                
            # Actualizar PID en la configuración de la VM
            vm['qemu_pid'] = int(stdout.split()[0])
            Logger.success(f"VM ID-{vm['id']} iniciada con PID {vm['qemu_pid']}")
            
        except Exception as e:
            Logger.error(f"Error iniciando VM {vm['id']}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _generate_image_commands(self, vms: list, slice_config: dict) -> list:
        """
        Genera comandos para preparar las imágenes de las VMs.
        
        Para cada VM:
        1. Copia la imagen base
        2. Redimensiona según el flavor
        3. Configura permisos

        Args:
            vms (list): Lista de VMs a configurar
            slice_config (dict): Configuración completa del slice

        Returns:
            list: Comandos para preparar imágenes
        """
        Logger.debug("Generando comandos de preparación de imágenes")
        commands = []
        
        for vm in vms:
            image_id = str(vm['image_id'])
            flavor_id = str(vm['flavor_id'])
            
            image_info = slice_config['resources_info']['images'][image_id]
            flavor_info = slice_config['resources_info']['flavors'][flavor_id]
            
            Logger.debug(f"VM ID-{vm['id']}: Imagen {image_id}, Flavor {flavor_id}")
            
            # Generar nombres de archivo
            base_image = os.path.basename(image_info['path'])
            vm_image = f"vm-{str(vm['id'])}-slice-{str(slice_config['slice_info']['id'])}.qcow2"
            vm_image_path = os.path.join('/home/ubuntu/SliceManager/images', vm_image)
            base_image_path = os.path.join('/home/ubuntu/SliceManager/base', base_image)
            
            Logger.debug(f"Base: {base_image_path}")
            Logger.debug(f"VM: {vm_image_path}")

            disk_size_mb = int(flavor_info['disk'] * 1024)

            commands.extend([
                f"test -f {base_image_path} || exit 1",
                f"sudo cp {base_image_path} {vm_image_path}",
                f"sudo qemu-img resize --shrink {vm_image_path} {disk_size_mb}M",
                f"sudo chmod 644 {vm_image_path}"
            ])
        
        return commands

    def _setup_switches(self):
        """
        Configura los switches OVS en todos los nodos del clúster.
        
        Configura:
        1. Bridge principal en cada nodo
        2. Interfaces físicas:
        - ens5-ens8 en OFS
        - ens4 en workers
        """
        Logger.section("CONFIGURANDO SWITCHES OVS")
        
        for node_name in NODES:
            Logger.info(f"Configurando switch en {node_name}...")
            with SSHManager(node_name) as ssh:
                # Agregar interfaces físicas según el tipo de nodo
                if node_name == 'ofs':
                    Logger.debug("Configurando interfaces OFS")
                    for i in range(5, 9):
                        ssh.execute(f"sudo ovs-vsctl add-port {NODES[node_name]['switch']} ens{i}")
                        Logger.debug(f"Agregada interfaz ens{i}")
                else:
                    Logger.debug("Configurando interfaz worker")
                    ssh.execute(f"sudo ovs-vsctl add-port {NODES[node_name]['switch']} ens4")
                    Logger.debug("Agregada interfaz ens4")
                    
            Logger.success(f"Switch {node_name} configurado")

    def _enable_qinq_support(self):
        """
        Habilita soporte QinQ en todos los switches OVS.
        
        Configura el límite de VLANs a 2 para permitir QinQ (VLAN stacking).
        """
        Logger.section("HABILITANDO SOPORTE QinQ")
        
        for node_name in NODES:
            Logger.info(f"Configurando QinQ en {node_name}...")
            with SSHManager(node_name) as ssh:
                ssh.execute("sudo ovs-vsctl set Open_vSwitch . other_config:vlan-limit=2")
            Logger.success(f"QinQ habilitado en {node_name}")

    def _configure_physical_ports(self):
        """
        Configura los puertos físicos como trunks en todos los nodos.
        
        Configura:
        1. ens4 como trunk en workers
        2. ens5-ens8 como trunks en OFS
        """
        Logger.section("CONFIGURANDO PUERTOS FÍSICOS")
        
        # Configurar workers
        Logger.info("Configurando workers...")
        for node_name in [n for n in NODES if n != 'ofs']:
            with SSHManager(node_name) as ssh:
                ssh.execute("sudo ovs-vsctl set port ens4 vlan_mode=trunk")
                Logger.debug(f"Puerto ens4 configurado como trunk en {node_name}")
        
        # Configurar OFS
        Logger.info("Configurando OFS...")
        with SSHManager('ofs') as ssh:
            for port in ['ens5', 'ens6', 'ens7', 'ens8']:
                ssh.execute(f"sudo ovs-vsctl set port {port} vlan_mode=trunk")
                Logger.debug(f"Puerto {port} configurado como trunk en OFS")
                
        Logger.success("Puertos físicos configurados exitosamente")

    def _setup_internet_access(self, network_config: dict):
        """
        Configura acceso a internet y DHCP para el slice.
        
        Realiza la configuración completa:
        1. Crea y configura namespace de red
        2. Configura veth pairs y bridges
        3. Configura NAT y forwarding
        4. Configura servidor DHCP
        
        Args:
            network_config (dict): Configuración de red con:
                - slice_id: ID único del slice
                - svlan_id: ID del SVLAN
                - network: Red del slice (CIDR)
                - dhcp_interface: Nombre interfaz DHCP
                - gateway_interface: Nombre interfaz gateway
        """
        # LOCK GLOBAL PARA EVITAR CONDICIONES DE CARRERA
        with network_setup_lock:
            Logger.section("CONFIGURANDO ACCESO A INTERNET")

            # Extraer información base
            slice_id = network_config['slice_id']
            svlan = network_config['svlan_id']
            network = network_config['network']
            gateway_if = network_config['gateway_interface']

            # Generar nombres únicos por slice
            namespace_name = f"ns-slice-{slice_id}"
            veth_int_name = f"veth-int-{slice_id}"
            veth_ext_name = f"veth-ext-{slice_id}"
            dhcp_if = f"veth-int.{svlan}"

            Logger.info(f"Slice ID: {slice_id}")
            Logger.info(f"SVLAN: {svlan}")
            Logger.info(f"Red: {network}")
            Logger.info(f"Namespace: {namespace_name}")

            try:
                # 1. Verificar si namespace ya existe (evitar conflictos)
                result = subprocess.run(['ip', 'netns', 'list'], capture_output=True, text=True)
                if namespace_name in result.stdout:
                    Logger.warning(f"Namespace {namespace_name} ya existe, eliminando...")
                    os.system(f"sudo ip netns del {namespace_name} 2>/dev/null || true")

                # 2. Configurar namespace y veth pair únicos
                Logger.subsection("CONFIGURANDO NAMESPACE Y VETH PAIR")

                # Eliminar interfaces existentes si existen
                os.system(f"sudo ip link del {veth_ext_name} 2>/dev/null || true")

                os.system(f"sudo ip netns add {namespace_name}")
                os.system(f"sudo ip link add {veth_int_name} type veth peer name {veth_ext_name}")
                os.system(f"sudo ip link set {veth_int_name} netns {namespace_name}")

                # 3. Configurar interfaces
                Logger.subsection("CONFIGURANDO INTERFACES")
                os.system(f"sudo ip netns exec {namespace_name} ip link set {veth_int_name} up")
                os.system(f"sudo ip netns exec {namespace_name} ip link set lo up")
                os.system(f"sudo ip link set {veth_ext_name} up")

                # Verificar que el bridge existe antes de agregar puerto
                bridge_exists = subprocess.run(['sudo', 'ovs-vsctl', 'br-exists', NETWORK_CONFIG['headnode_br']],
                                               capture_output=True)
                if bridge_exists.returncode != 0:
                    Logger.warning(f"Bridge {NETWORK_CONFIG['headnode_br']} no existe, creándolo...")
                    os.system(f"sudo ovs-vsctl add-br {NETWORK_CONFIG['headnode_br']}")

                os.system(f"sudo ovs-vsctl add-port {NETWORK_CONFIG['headnode_br']} {veth_ext_name}")

                # 4. Conectar a OVS y configurar gateway
                Logger.subsection("CONFIGURANDO OVS Y GATEWAY")

                # Eliminar puerto existente si existe
                os.system(f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {gateway_if}")

                os.system(f"sudo ovs-vsctl --may-exist add-port {NETWORK_CONFIG['headnode_br']} {gateway_if}")
                os.system(f"sudo ovs-vsctl set interface {gateway_if} type=internal")
                os.system(f"sudo ovs-vsctl set port {gateway_if} tag={svlan}")
                os.system(f"sudo ip link set {gateway_if} up")

                # Verificar si IP ya está asignada
                ip_check = subprocess.run(['ip', 'addr', 'show', gateway_if], capture_output=True, text=True)
                gateway_ip = network.replace('0/24', '1/24')
                if gateway_ip.split('/')[0] not in ip_check.stdout:
                    os.system(f"sudo ip addr add {gateway_ip} dev {gateway_if}")

                # 5. Configurar VLAN subinterface dentro del namespace
                Logger.subsection("CONFIGURANDO VLAN SUBINTERFACE")
                os.system(
                    f"sudo ip netns exec {namespace_name} ip link add link {veth_int_name} name {dhcp_if} type vlan id {svlan}")
                os.system(f"sudo ip netns exec {namespace_name} ip link set {dhcp_if} up")
                os.system(
                    f"sudo ip netns exec {namespace_name} ip addr add {network.replace('0/24', '2/24')} dev {dhcp_if}")

                # 6. Configurar NAT (verificar existencia antes de agregar)
                Logger.subsection("CONFIGURANDO NAT Y FORWARDING")
                nat_commands = [
                    f"sudo ip netns exec {namespace_name} iptables -t nat -C POSTROUTING -s {network} -o {NETWORK_CONFIG['internet_interface']} -j MASQUERADE 2>/dev/null || sudo ip netns exec {namespace_name} iptables -t nat -A POSTROUTING -s {network} -o {NETWORK_CONFIG['internet_interface']} -j MASQUERADE",
                    f"sudo iptables -C FORWARD -i {gateway_if} -o {NETWORK_CONFIG['internet_interface']} -s {network} -j ACCEPT 2>/dev/null || sudo iptables -A FORWARD -i {gateway_if} -o {NETWORK_CONFIG['internet_interface']} -s {network} -j ACCEPT",
                    f"sudo iptables -C FORWARD -i {NETWORK_CONFIG['internet_interface']} -o {gateway_if} -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null || sudo iptables -A FORWARD -i {NETWORK_CONFIG['internet_interface']} -o {gateway_if} -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT",
                    "sudo sysctl -w net.ipv4.ip_forward=1"
                ]

                for cmd in nat_commands:
                    Logger.debug(f"Ejecutando: {cmd}")
                    result = os.system(cmd)
                    if result != 0:
                        Logger.warning(f"Comando NAT falló: {cmd}")

                # 7. Configurar DHCP
                Logger.subsection("CONFIGURANDO DHCP")
                dhcp_config = self._generate_dhcp_config(network_config, namespace_name)
                self._apply_dhcp_config(dhcp_config, svlan, namespace_name)

                Logger.success("Configuración de red completada exitosamente")

            except Exception as e:
                Logger.error(f"Error en configuración de red: {str(e)}")
                # Limpiar recursos parciales en caso de error
                try:
                    os.system(f"sudo ip netns del {namespace_name} 2>/dev/null || true")
                    os.system(f"sudo ip link del {veth_ext_name} 2>/dev/null || true")
                    os.system(f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {gateway_if}")
                except:
                    pass
                raise
    
    def _generate_dhcp_config(self, network_config: dict, namespace_name: str) -> str:
        """
        Genera la configuración del servidor DHCP para el slice.
        
        Args:
            network_config (dict): Configuración de red del slice
            namespace_name (str): Nombre único del namespace para este slice

        Returns:
            str: Contenido del archivo de configuración DHCP
        """
        Logger.debug("Generando configuración DHCP")

        network = network_config['network']
        svlan = network_config['svlan_id']
        range1 = network_config['dhcp_range'][0]
        range2 = network_config['dhcp_range'][1]
        dhcp_ip = network.replace('0/24', '2')
        dhcp_if = f"veth-int.{svlan}"

        # Definir rutas de archivos únicos por slice
        log_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.log')
        lease_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.leases')
        pid_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.pid')

        Logger.debug(f"Namespace: {namespace_name}")
        Logger.debug(f"SVLAN: {svlan}")
        Logger.debug(f"Red: {network}")
        Logger.debug(f"Rango DHCP: {range1} - {range2}")
        Logger.debug(f"IP DHCP: {dhcp_ip}")
        Logger.debug(f"Interface DHCP: {dhcp_if}")
        
        # Generar configuración en una sola línea para cada opción
        config = [
            f"interface={dhcp_if}",
            f"listen-address={dhcp_ip}",
            "bind-interfaces",
            "except-interface=lo",
            "no-hosts",
            "no-resolv",
            f"dhcp-range={range1},{range2},255.255.255.0,12h",
            f"dhcp-option=3,{network.replace('0/24', '1')}",  # Router
            "dhcp-option=1,255.255.255.0",  # Netmask
            f"dhcp-option=28,{network.rsplit('.', 1)[0]}.255",  # Broadcast
            "dhcp-option=6,8.8.8.8,8.8.4.4",  # DNS Servers
            f"log-facility={log_file}",
            f"pid-file={pid_file}",
            "dhcp-authoritative",
            f"dhcp-leasefile={lease_file}",
            "log-queries",  # Para debug
            "log-dhcp"     # Para debug
        ]
            
        # Unir configuración con saltos de línea
        final_config = '\n'.join(config)
        
        Logger.debug("Configuración DHCP generada:")
        Logger.debug(f"\n{final_config}")
        
        return final_config

    def _apply_dhcp_config(self, config: str, svlan: int, namespace_name: str):
        """
        Aplica la configuración DHCP generada.
        
        1. Guarda la configuración en archivo
        2. Inicia servidor DHCP (dnsmasq)
        3. Verifica estado del servicio
        
        Args:
            config (str): Configuración DHCP a aplicar
            svlan (int): ID del SVLAN para nombrar archivos
            namespace_name (str): Namespace donde ejecutar dnsmasq
            
        Raises:
            Exception: Si hay errores iniciando el servidor DHCP
        """
        Logger.subsection("APLICANDO CONFIGURACIÓN DHCP")
        
        try:
            # Guardar config en archivo
            config_file = os.path.join(
                DATA_DIR['dhcp']['config'],
                f'dnsmasq.{svlan}.conf'
            )
            Logger.info(f"Guardando configuración en {config_file}")
            
            with open(config_file, 'w') as f:
                f.write(config)

            # Ejecutar dnsmasq en el namespace específico del slice
            Logger.info(f"Iniciando servidor DHCP en namespace {namespace_name}...")
            result = os.system(
                f"sudo ip netns exec {namespace_name} "
                f"dnsmasq -C {config_file}"
            )

            # Verificar resultado
            if result != 0:
                # Verificar error específico
                verify_cmd = (f"sudo ip netns exec {namespace_name} "
                              f"dnsmasq -C {config_file} 2>&1")
                output = subprocess.getoutput(verify_cmd)

                if "failed to create listening socket" in output and "Address already in use" in output:
                    Logger.error("Puerto DHCP ya en uso en este namespace")
                    raise Exception("DHCP port already in use")
                else:
                    Logger.error(f"Error iniciando servidor DHCP: {output}")
                    raise Exception(f"Failed to start DHCP server: {output}")

            Logger.success("Servidor DHCP iniciado exitosamente")
                
        except Exception as e:
            # Limpiar archivo de configuración en caso de error
            try:
                os.remove(config_file)
                Logger.debug(f"Archivo de configuración eliminado: {config_file}")
            except:
                pass
            Logger.error(f"Error aplicando configuración DHCP: {str(e)}")
            raise

    def _start_vms(self, vms: list, slice_config: dict):
        """
        Inicia las VMs en sus respectivos workers.

        Realiza el proceso completo de inicio:
        1. Identifica y transfiere las imágenes necesarias a cada worker
        2. Crea y configura las imágenes de VM
        3. Inicia las VMs con QEMU/KVM
        4. Limpia recursos temporales

        Args:
            vms (list): Lista de VMs a iniciar
            slice_config (dict): Configuración completa del slice

        Raises:
            Exception: Si hay errores iniciando las VMs o en la limpieza
        """
        try:
            Logger.section("INICIANDO VMs")
            
            # 1. Identificar imágenes necesarias por worker
            Logger.info("Identificando imágenes necesarias")
            worker_images = {}  # {worker_name: set(image_paths)}
            
            for vm in vms:
                worker = vm['physical_server']['name']
                image_path = self.resource_manager.get_image_path(vm['image_id'])
                Logger.debug(f"VM ID-{vm['id']}: {image_path} -> {worker}")
                
                worker_images.setdefault(worker, set()).add(image_path)

            # 2. Copiar imágenes base a workers
            Logger.section("COPIANDO IMÁGENES A WORKERS")
            for worker, images in worker_images.items():
                Logger.subsection(f"Worker: {worker}")
                with SSHManager(worker) as ssh:
                    # Preparar directorio
                    Logger.info("Configurando directorios...")
                    ssh.execute(f"sudo mkdir -p {DATA_DIR['images']}")
                    ssh.execute(f"sudo chmod 777 {DATA_DIR['images']}")
                    
                    # Transferir imágenes
                    sftp = ssh.client.open_sftp()
                    for image_path in images:
                        base_name = os.path.basename(image_path)
                        remote_path = f"{DATA_DIR['images']}/{base_name}"
                        
                        try:
                            sftp.stat(remote_path)
                            Logger.debug(f"Imagen {base_name} ya existe")
                        except FileNotFoundError:
                            Logger.info(f"Transfiriendo {base_name}...")
                            sftp.put(image_path, remote_path)
                    
                    sftp.close()
                    
                    # Restaurar permisos
                    ssh.execute(f"sudo chmod 755 {DATA_DIR['images']}")
                    ssh.execute(f"sudo chown -R ubuntu:ubuntu {DATA_DIR['images']}")
                    Logger.success(f"Imágenes copiadas a {worker}")

            # 3. Crear y configurar VMs
            Logger.section("CONFIGURANDO E INICIANDO VMs")
            for vm in vms:
                Logger.subsection(f"VM ID-{vm['id']} en {vm['physical_server']['name']}")
                worker = vm['physical_server']['name']
                
                with SSHManager(worker) as ssh:
                    # Obtener configuración
                    flavor = self.resource_manager.get_flavor_config(vm['flavor_id'])
                    base_image = os.path.basename(
                        self.resource_manager.get_image_path(vm['image_id'])
                    )
                    
                    # Preparar imagen
                    vm_image = f"vm-{vm['id']}-slice-{slice_config['slice_info']['id']}.qcow2"
                    vm_image_path = os.path.join(DATA_DIR['images'], vm_image)
                    
                    Logger.info("Preparando imagen...")
                    Logger.debug(f"Base: {base_image}")
                    Logger.debug(f"Destino: {vm_image_path}")
                    Logger.debug(f"Tamaño: {flavor['disk']} GB")

                    # Copiar y redimensionar
                    disk_size_mb = int(flavor['disk'] * 1024)

                    ssh.execute(f"sudo cp {DATA_DIR['images']}/{base_image} {vm_image_path}")
                    ssh.execute(f"sudo qemu-img resize --shrink {vm_image_path} {disk_size_mb}M")
                    
                    stdout, _ = ssh.execute(f"sudo qemu-img info {vm_image_path}")
                    Logger.debug(f"Información de imagen:\n{stdout}")

                    # Construir comando QEMU
                    cmd = [
                        "sudo qemu-system-x86_64",
                        "-enable-kvm",
                        f"-name guest=VM{vm['id']}-S{slice_config['slice_info']['id']}",
                        f"-m {flavor['ram']}",
                        f"-smp {flavor['vcpus']}",
                        f"-drive file={vm_image_path},format=qcow2",
                        f"-vnc 0.0.0.0:{vm['vnc_display']}",
                        "-daemonize"
                    ]

                    # Configurar interfaces
                    vm_interfaces = [
                        i for i in slice_config['topology_info']['interfaces']
                        if i['vm_id'] == vm['id']
                    ]
                    vm_interfaces.sort(key=lambda x: not x['external_access'])
                    
                    Logger.info("Configurando interfaces...")
                    for interface in vm_interfaces:
                        tap_name = interface['tap_name']
                        cmd.extend([
                            f"-netdev tap,id={tap_name},ifname={tap_name},script=no,downscript=no",
                            f"-device e1000,netdev={tap_name},mac={interface['mac_address']}"
                        ])
                        Logger.debug(f"Interface: {tap_name}, MAC: {interface['mac_address']}")

                    # Iniciar VM
                    Logger.info("Ejecutando QEMU...")
                    Logger.debug(f"Comando: {' '.join(cmd)}")
                    
                    stdout, stderr = ssh.execute(' '.join(cmd))
                    if stderr:
                        Logger.warning(f"QEMU stderr: {stderr}")

                    # Verificar estado
                    time.sleep(2)
                    stdout, _ = ssh.execute(
                        f"pgrep -fa 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'"
                    )
                    
                    if stdout.strip():
                        vm['qemu_pid'] = int(stdout.strip())
                        Logger.success(f"VM ID-{vm['id']} iniciada con PID {vm['qemu_pid']}")
                    else:
                        raise Exception(
                            f"No se pudo obtener PID de QEMU para VM {vm['id']}"
                        )

            # 4. Limpiar imágenes base
            Logger.section("LIMPIANDO RECURSOS TEMPORALES")
            for worker, images in worker_images.items():
                with SSHManager(worker) as ssh:
                    for image_path in images:
                        base_name = os.path.basename(image_path)
                        Logger.debug(f"Limpiando {base_name} de {worker}")
                        ssh.execute(f"sudo rm -f {DATA_DIR['images']}/{base_name}")
            
            Logger.success("Inicio de VMs completado exitosamente")

        except Exception as e:
            Logger.error(f"Error iniciando VMs: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            
            # Intentar limpiar recursos
            Logger.section("INICIANDO LIMPIEZA POR ERROR")
            for worker, images in worker_images.items():
                try:
                    with SSHManager(worker) as ssh:
                        Logger.info(f"Limpiando recursos en {worker}...")
                        # Limpiar imágenes base
                        for image_path in images:
                            base_name = os.path.basename(image_path)
                            ssh.execute(f"sudo rm -f {DATA_DIR['images']}/{base_name}")
                        # Limpiar imágenes de VM
                        ssh.execute(f"sudo rm -f {DATA_DIR['images']}/vm-*-slice-*.qcow2")
                except Exception as cleanup_error:
                    Logger.warning(
                        f"Error durante limpieza en {worker}: {str(cleanup_error)}"
                    )
            
            raise Exception(f"Error iniciando VMs: {str(e)}")

    def stop_slice(self, slice_id: int, request_data: dict) -> tuple:
        """
        Detiene todas las VMs de un slice y limpia sus recursos.
        
        Realiza las siguientes operaciones:
        1. Detiene las VMs en cada worker (primero intenta shutdown limpio)
        2. Limpia recursos de red (bridges, TAPs, etc)
        3. Limpia archivos de imagen
        4. Actualiza estado del slice
        
        Args:
            slice_id (int): ID del slice a detener
            request_data (dict): Datos de configuración con:
                - vms: Lista de VMs a detener
                - workers: Información de workers

        Returns:
            tuple: (success: bool, results: dict)
                - success: True si todas las operaciones fueron exitosas
                - results: Resultados de operación por worker
        """
        try:
            Logger.section(f"DETENIENDO SLICE {slice_id}")
            Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

            # Limpieza en el HeadNode

            # Obtener información del slice
            network_config = request_data['network_config']
            svlan = network_config['svlan_id']
            network = network_config['network']
            gateway_if = network_config['gateway_interface']

            # Generar nombres únicos del slice
            namespace_name = f"ns-slice-{slice_id}"
            veth_int_name = f"veth-int-{slice_id}"
            veth_ext_name = f"veth-ext-{slice_id}"

            # Matar dnsmasq específico del slice (SIN CAMBIOS)
            cmd = f"pgrep -f 'dnsmasq.{svlan}.conf'"
            try:
                pid = subprocess.check_output(cmd, shell=True).decode().strip()
                if pid:
                    Logger.info(f"Matando proceso dnsmasq (PID: {pid})")
                    os.system(f"sudo kill -9 {pid}")
            except subprocess.CalledProcessError:
                Logger.debug("No se encontró proceso dnsmasq activo")

            # 1. Eliminar interfaces del bridge OVS
            Logger.info("Eliminando interfaces específicas del slice...")
            ovs_cleanup_commands = [
                f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {veth_ext_name}",
                f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {gateway_if}"
            ]
            for cmd in ovs_cleanup_commands:
                os.system(cmd)

            # 2. Eliminar interfaces del host
            Logger.info("Eliminando interfaces del host...")
            host_cleanup_commands = [
                f"sudo ip link del {gateway_if} 2>/dev/null || true",
                f"sudo ip link del {veth_ext_name} 2>/dev/null || true"
            ]
            for cmd in host_cleanup_commands:
                os.system(cmd)

            # 3. Eliminar interfaces dentro del namespace
            Logger.info(f"Eliminando namespace {namespace_name}...")
            ns_cleanup_commands = [
                f"sudo ip netns exec {namespace_name} ip link del veth-int.{svlan} 2>/dev/null || true",
                f"sudo ip netns del {namespace_name} 2>/dev/null || true"
            ]
            for cmd in ns_cleanup_commands:
                os.system(cmd)

            # 4. Limpiar reglas de iptables y forwarding
            Logger.info("Limpiando reglas de red específicas...")
            cleanup_commands = [
                f"sudo iptables -t nat -D POSTROUTING -s {network} -o {NETWORK_CONFIG['internet_interface']} -j MASQUERADE 2>/dev/null || true",
                f"sudo iptables -D FORWARD -i {gateway_if} -o {NETWORK_CONFIG['internet_interface']} -s {network} -j ACCEPT 2>/dev/null || true",
                f"sudo iptables -D FORWARD -i {NETWORK_CONFIG['internet_interface']} -o {gateway_if} -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null || true",
                "sudo sysctl -w net.ipv4.ip_forward=1"
            ]
            for cmd in cleanup_commands:
                os.system(cmd)

            # 5. Limpiar configuración DHCP
            Logger.info("Limpiando configuración DHCP específica...")
            dhcp_config = os.path.join(DATA_DIR['dhcp']['config'], f'dnsmasq.{svlan}.conf')
            if os.path.exists(dhcp_config):
                os.remove(dhcp_config)

            # Agrupar VMs por worker
            vms_by_worker = {}
            for vm in request_data['vms']:
                worker_id = str(vm['physical_server']['id'])
                worker_info = request_data['workers'][worker_id]
                
                vms_by_worker.setdefault(worker_info['name'], {
                    'info': worker_info,
                    'vms': [],
                    'network_config': request_data['network_config'],  # Incluir configuración de red
                    'interfaces': [  # Incluir interfaces relacionadas a las VMs de este worker
                        iface for iface in request_data['interfaces']
                        if iface['vm_id'] == vm['id']
                    ]
                })['vms'].append(vm)

            # Almacenamiento de resultados
            worker_results = {}
            worker_errors = {}

            def stop_worker_vms(worker_name: str, worker_data: dict):
                """Función interna para detener VMs en un worker específico"""
                try:
                    worker_info = worker_data['info']
                    vms = worker_data['vms']
                    network_config = worker_data['network_config']
                    interfaces = worker_data['interfaces']
                    slice_bridge = network_config['slice_bridge_name']
                    
                    Logger.subsection(f"Deteniendo VMs en {worker_name}")

                    ssh_info = {
                        'name': worker_info['name'],
                        'ip': worker_info['ip'],
                        'ssh_username': worker_info['ssh_username'],
                        'ssh_password': worker_info.get('ssh_password'),
                        'ssh_key_path': worker_info.get('ssh_key_path')
                    }

                    with SSHManager(ssh_info) as ssh:
                        # 1. Detener VMs
                        for vm in vms:
                            try:
                                Logger.info(f"Deteniendo VM ID-{vm['id']}")
                                
                                # Verificar si proceso existe
                                stdout, _ = ssh.execute(
                                    f"pgrep -fa 'guest=VM{vm['id']}-S{slice_id}'"
                                )
                                if stdout.strip():
                                    qemu_pid = stdout.split()[0]
                                    Logger.debug(f"QEMU PID {qemu_pid}")
                                    
                                    # Intentar shutdown limpio
                                    try:
                                        ssh.execute(f"sudo kill {qemu_pid}")
                                        time.sleep(2)  # Esperar shutdown
                                    except Exception as e:
                                        if "No such process" in str(e):
                                            Logger.debug(f"Proceso {qemu_pid} ya terminado")
                                            continue
                                        Logger.warning(f"Error en kill: {str(e)}")
                                        
                                    # Verificar si sigue corriendo
                                    stdout, _ = ssh.execute(
                                        f"pgrep -f 'guest=VM{vm['id']}-S{slice_id}'"
                                    )
                                    if stdout.strip():
                                        Logger.warning(f"VM ID-{vm['id']} sigue corriendo, usando kill -9")
                                        ssh.execute(f"sudo kill -9 {qemu_pid}")
                                        time.sleep(1)
                                    else:
                                        Logger.success(f"VM ID-{vm['id']} detenida exitosamente")
                                else:
                                    Logger.debug(f"VM ID-{vm['id']} ya no está corriendo")

                            except Exception as e:
                                if "No such process" not in str(e) and "status=1" not in str(e):
                                    raise
                                Logger.debug(f"VM ID-{vm['id']} ya no está corriendo")

                        # 2. Limpiar recursos
                        cleanup_commands = []
            
                        # Eliminar TAP interfaces
                        for iface in interfaces:
                            if iface['external_access']:
                                cleanup_commands.append(f"sudo ovs-vsctl --if-exists del-port br-int {iface['tap_name']}")
                            else:
                                cleanup_commands.append(f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {iface['tap_name']}")
                            cleanup_commands.append(f"sudo ip link del {iface['tap_name']} 2>/dev/null || true")

                        # Eliminar bridges y patch ports
                        cleanup_commands.extend([
                            f"sudo ovs-vsctl --if-exists del-port br-int {network_config['patch_ports']['int_side']}",
                            f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {network_config['patch_ports']['slice_side']}",
                            f"sudo ovs-vsctl --if-exists del-br {slice_bridge}"
                        ])
                    
                        Logger.info(f"Ejecutando limpieza en {worker_name}")
                        try:
                            ssh.execute(" && ".join(cleanup_commands))
                        except Exception as e:
                            Logger.warning(f"Error no fatal durante limpieza: {str(e)}")
                        
                        worker_results[worker_name] = {"success": True}
                        Logger.success(f"Worker {worker_name} limpiado exitosamente")

                except Exception as e:
                    Logger.error(f"Error en worker {worker_name}: {str(e)}")
                    if "No such process" not in str(e) and "status=1" not in str(e):
                        worker_errors[worker_name] = str(e)
                        worker_results[worker_name] = {
                            "success": False,
                            "error": str(e)
                        }
                    else:
                        Logger.debug(f"Error no fatal, continuando...")
                        worker_results[worker_name] = {"success": True}

            # Ejecutar limpieza en paralelo
            threads = []
            for worker_name, worker_data in vms_by_worker.items():
                thread = threading.Thread(
                    target=stop_worker_vms,
                    args=(worker_name, worker_data)
                )
                threads.append(thread)
                thread.start()

            # Esperar threads
            for thread in threads:
                thread.join()

            # Verificar resultados
            failed_workers = [
                worker for worker, result in worker_results.items()
                if not result["success"]
            ]

            if failed_workers:
                error_messages = [
                    f"{worker}: {worker_results[worker]['error']}"
                    for worker in failed_workers
                ]
                raise Exception("Errores deteniendo workers:\n" + "\n".join(error_messages))

            Logger.success(f"Slice {slice_id} detenido exitosamente")
            return True, worker_results

        except Exception as e:
            Logger.error(f"Error en stop_slice: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, {"error": str(e)}
   
    def restart_slice(self, slice_id: int, request_data: dict) -> tuple:
        """
        Reinicia todas las VMs de un slice en paralelo.
        """
        try:
            Logger.section(f"REINICIANDO SLICE {slice_id}")
            Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

            # 1. Obtener VNC displays disponibles
            Logger.info("Obteniendo displays VNC disponibles")
            worker_ids = [vm['physical_server']['id'] for vm in request_data['vms']]
            Logger.debug(f"Worker IDs solicitados: {worker_ids}")
            
            # Obtener display VNC
            slice_manager = get_service_instance('slice-manager')
            if not slice_manager:
                raise Exception("Servicio slice-manager no disponible")
            
            Logger.info("Obteniendo displays VNC...")
            vnc_response = requests.post(
                f"http://{slice_manager['ipAddr']}:{slice_manager['port']}/get-available-vnc-displays",
                json={'worker_ids': worker_ids}
            )

            if vnc_response.status_code != 200:
                raise Exception("Error obteniendo VNC displays disponibles")

            available_displays = vnc_response.json()['content']
            Logger.debug(f"Displays disponibles por worker: {json.dumps(available_displays, indent=2)}")

            # 2. Agrupar VMs por worker y pre-asignar displays
            vms_by_worker = {}
            display_assignments = {}  # Mantener registro de asignaciones {worker_id: {vm_id: display}}
            
            for worker_id in set(str(vm['physical_server']['id']) for vm in request_data['vms']):
                display_assignments[worker_id] = {}
                worker_displays = available_displays[worker_id]
                Logger.debug(f"Worker ID-{worker_id} tiene {len(worker_displays)} displays disponibles: {worker_displays}")
                
                # Obtener VMs para este worker
                worker_vms = [vm for vm in request_data['vms'] if str(vm['physical_server']['id']) == worker_id]
                Logger.debug(f"Worker ID-{worker_id} tiene {len(worker_vms)} VMs para reiniciar")
                
                if len(worker_vms) > len(worker_displays):
                    raise Exception(f"No hay suficientes displays para worker {worker_id}. " 
                                f"Necesita {len(worker_vms)}, disponibles {len(worker_displays)}")
                
                # Pre-asignar displays únicos a cada VM
                for idx, vm in enumerate(worker_vms):
                    display = worker_displays[idx]
                    display_assignments[worker_id][vm['id']] = display
                    Logger.debug(f"Pre-asignando display {display} a VM ID-{vm['id']} en Worker ID-{worker_id}")
                
                # Guardar información en vms_by_worker
                vms_by_worker[worker_id] = {
                    'worker_info': request_data['workers'][worker_id],
                    'vms': worker_vms,
                    'displays': worker_displays
                }

            def process_worker_vms(worker_id: str, worker_data: dict, results: dict):
                try:
                    Logger.subsection(f"Procesando worker {worker_id}")
                    worker_vms = []

                    # Pausar VMs
                    for vm in worker_data['vms']:
                        Logger.info(f"Pausando VM ID-{vm['id']}")
                        self.pause_vm({
                            "vm_info": {
                                "id": vm['id'],
                                "name": vm['name'],
                                "qemu_pid": vm['qemu_pid'],
                                "vnc_display": vm['vnc_display'],
                                "status": vm['status']
                            },
                            "worker_info": worker_data['worker_info']
                        })

                    # Reanudar VMs con sus displays asignados
                    for vm in worker_data['vms']:
                        # Usar el display pre-asignado para esta VM
                        assigned_display = display_assignments[worker_id][vm['id']]
                        Logger.info(f"Reanudando VM ID-{vm['id']} con display {assigned_display} en Worker ID-{worker_id}")
                        
                        # Verificar si el display ya está en uso
                        Logger.debug(f"Verificando si display {assigned_display} está en uso...")
                        success, qemu_pid, display = self.resume_vm({
                            "vm_info": {
                                "id": vm['id'],
                                "name": vm['name'],
                                "image_path": vm['image_path'],
                                "status": "paused",
                                "flavor": vm['flavor'],
                                "vnc_display": assigned_display,  # Usar el display pre-asignado
                                "interfaces": vm['interfaces']
                            },
                            "worker_info": worker_data['worker_info'],
                            "slice_id": slice_id
                        })

                        if success:
                            worker_vms.append({
                                "id": vm['id'],
                                "qemu_pid": qemu_pid,
                                "vnc_display": assigned_display,  # Mantener el display asignado
                                "vnc_port": 5900 + assigned_display,
                                "status": "running"
                            })
                            Logger.debug(f"VM ID-{vm['id']} reiniciada exitosamente con display {assigned_display}")

                    results[worker_id] = {
                        "success": True,
                        "vms": worker_vms
                    }

                except Exception as e:
                    Logger.error(f"Error en worker {worker_id}: {str(e)}")
                    results[worker_id] = {
                        "success": False,
                        "error": str(e)
                    }

            # 3. Procesar workers en paralelo
            worker_results = {}
            threads = []

            for worker_id, worker_data in vms_by_worker.items():
                thread = threading.Thread(
                    target=process_worker_vms,
                    args=(worker_id, worker_data, worker_results)
                )
                threads.append(thread)
                thread.start()

            # Esperar threads
            for thread in threads:
                thread.join()

            # 4. Procesar y validar resultados
            updated_vms = []
            errors = []
            assigned_displays_summary = {}  # Para verificar asignaciones finales

            for worker_id, result in worker_results.items():
                if result["success"]:
                    worker_vms = result["vms"]
                    updated_vms.extend(worker_vms)
                    
                    # Verificar asignaciones únicas por worker
                    worker_displays = set()
                    for vm in worker_vms:
                        if vm["vnc_display"] in worker_displays:
                            errors.append(f"Display duplicado {vm['vnc_display']} en worker {worker_id}")
                        worker_displays.add(vm["vnc_display"])
                    
                    assigned_displays_summary[worker_id] = {
                        "displays": list(worker_displays),
                        "vm_assignments": {vm["id"]: vm["vnc_display"] for vm in worker_vms}
                    }
                else:
                    errors.append(f"Error en worker {worker_id}: {result.get('error', 'Unknown error')}")

            # Log resumen final de asignaciones
            Logger.section("RESUMEN DE ASIGNACIONES VNC")
            for worker_id, summary in assigned_displays_summary.items():
                Logger.info(f"Worker {worker_id}:")
                Logger.info(f"  Displays usados: {summary['displays']}")
                for vm_id, display in summary['vm_assignments'].items():
                    Logger.info(f"  VM ID-{vm_id} → Display N°{display}")

            if errors:
                raise Exception("\n".join(errors))

            Logger.success(f"Slice {slice_id} reiniciado exitosamente")
            return True, updated_vms

        except Exception as e:
            Logger.error(f"Error en restart_slice: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, []
        
    def pause_vm(self, request_data: dict) -> bool:
        """
        Pausa una VM específica deteniendo su proceso QEMU.

        Realiza el siguiente proceso:
        1. Verifica estado actual de la VM
        2. Obtiene acceso SSH al worker
        3. Verifica existencia del proceso QEMU
        4. Detiene el proceso de forma segura
        5. Verifica que el proceso fue detenido

        Args:
            request_data (dict): Datos necesarios con:
                - vm_info: Información de la VM a pausar
                - worker_info: Información del worker donde está la VM

        Returns:
            bool: True si la VM fue pausada exitosamente, False en caso de error
        """
        try:
            Logger.section(f"PAUSANDO VM")
            
            # Obtener y validar información
            vm_info = request_data['vm_info']
            worker_info = request_data['worker_info']
            
            Logger.debug(f"VM info: {json.dumps(vm_info, indent=2)}")
            Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")

            # Verificar estado actual
            if vm_info.get('status') == 'paused':
                Logger.info("VM ya está pausada")
                return True

            # Preparar conexión SSH
            ssh_info = {
                'name': worker_info['name'],
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            with SSHManager(ssh_info) as ssh:
                if not vm_info.get('qemu_pid'):
                    Logger.warning("No se proporcionó PID de QEMU")
                    return True

                Logger.info(f"Pausando proceso QEMU {vm_info['qemu_pid']}...")
                
                # Verificar si proceso existe
                stdout, _ = ssh.execute(
                    f"ps -p {vm_info['qemu_pid']} >/dev/null 2>&1 && echo 'running' || echo 'not found'"
                )
                
                if 'not found' in stdout:
                    Logger.debug(f"Proceso QEMU {vm_info['qemu_pid']} ya no existe")
                    return True

                # Detener proceso
                Logger.info("Deteniendo proceso QEMU...")
                ssh.execute(f"sudo kill -9 {vm_info['qemu_pid']}")
                
                # Verificar detención
                Logger.info("Verificando detención del proceso...")
                time.sleep(1)
                stdout, _ = ssh.execute(
                    f"ps -p {vm_info['qemu_pid']} >/dev/null 2>&1 && echo 'still running' || echo 'killed'"
                )
                
                if 'still running' in stdout:
                    raise Exception(f"No se pudo matar el proceso QEMU {vm_info['qemu_pid']}")
                
                Logger.success(f"Proceso QEMU {vm_info['qemu_pid']} detenido exitosamente")
                return True

        except Exception as e:
            Logger.error(f"Error pausando VM: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False

    def resume_vm(self, request_data: dict) -> Tuple[bool, int, int]:
        """
        Reanuda una VM pausada reiniciando su proceso QEMU.

        Realiza el siguiente proceso:
        1. Valida la información necesaria
        2. Obtiene un nuevo display VNC disponible
        3. Verifica la imagen de la VM
        4. Inicia nuevo proceso QEMU
        5. Verifica que el proceso inició correctamente

        Args:
            request_data (dict): Datos necesarios con:
                - vm_info: Información de la VM a reanudar
                - worker_info: Información del worker
                - slice_id: ID del slice
                - flavor: Configuración de recursos

        Returns:
            Tuple[bool, int, int]: (éxito, PID del proceso QEMU, número de display VNC)
        """
        try:
            Logger.section("REANUDANDO VM")
            
            # Obtener y validar información
            vm_info = request_data['vm_info']
            worker_info = request_data['worker_info']
            slice_id = request_data['slice_id']
            flavor_info = vm_info.get('flavor')
            
            Logger.debug(f"VM info: {json.dumps(vm_info, indent=2)}")
            Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")
            Logger.debug(f"Flavor info: {json.dumps(flavor_info, indent=2)}")

            # Validar flavor
            if not flavor_info or not all(k in flavor_info for k in ['ram', 'vcpus']):
                raise Exception("Información de flavor incompleta o no proporcionada")

            # Obtener display VNC
            slice_manager = get_service_instance('slice-manager')
            if not slice_manager:
                raise Exception("Servicio slice-manager no disponible")
            
            Logger.info("Obteniendo display VNC...")
            vnc_response = requests.post(
                f"http://{slice_manager['ipAddr']}:{slice_manager['port']}/get-available-vnc-displays",
                json={'worker_ids': [worker_info['id']]}
            )

            if vnc_response.status_code != 200:
                raise Exception("Error obteniendo VNC display disponible")

            available_displays = vnc_response.json()['content']
            worker_displays = available_displays.get(str(worker_info['id']), [])
            
            if not worker_displays:
                raise Exception("No hay VNC displays disponibles para este worker")
                
            if vm_info.get('vnc_display'):
                # Si ya tiene un display asignado, usarlo
                vnc_display = vm_info['vnc_display']
                Logger.debug(f"Usando display VNC existente: {vnc_display}")
            else:
                # Asignar un nuevo display VNC
                Logger.debug(f"Asignando nuevo display VNC: {worker_displays[0]}")
                vnc_display = worker_displays[0]
                
            Logger.debug(f"Display VNC asignado: {vnc_display}")

            # Preparar conexión SSH
            ssh_info = {
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            with SSHManager(ssh_info) as ssh:
                # Verificar si ya está corriendo
                try:
                    stdout, _ = ssh.execute(
                        f"pgrep -f 'guest=VM{vm_info['id']}-S{slice_id}'"
                    )
                    if stdout.strip():
                        Logger.info(f"VM ya está corriendo con PID {stdout.strip()}")
                        return True, int(stdout.strip()), vnc_display
                except Exception as e:
                    Logger.debug(f"No se encontró proceso QEMU existente (esperado)")

                # Verificar imagen
                vm_image = f"vm-{vm_info['id']}-slice-{slice_id}.qcow2"
                vm_image_path = f"/home/ubuntu/SliceManager/images/{vm_image}"

                stdout, _ = ssh.execute(f"test -f {vm_image_path} && echo 'exists'")
                if 'exists' not in stdout:
                    raise Exception(f"Imagen no encontrada: {vm_image_path}")

                # Construir comando QEMU
                Logger.info("Preparando comando QEMU...")
                cmd = [
                    "sudo qemu-system-x86_64",
                    "-enable-kvm",
                    f"-name guest=VM{vm_info['id']}-S{slice_id}",
                    f"-m {flavor_info['ram']}",
                    f"-smp {flavor_info['vcpus']}",
                    f"-drive file={vm_image_path},format=qcow2",
                    f"-vnc 0.0.0.0:{vnc_display}",
                    "-daemonize",
                    "-D /tmp/qemu.log",
                    "-d guest_errors"
                ]

                # Configurar interfaces
                Logger.info("Configurando interfaces...")
                vm_interfaces = sorted(
                    vm_info['interfaces'],
                    key=lambda x: not x['external_access']
                )

                for interface in vm_interfaces:
                    tap_name = interface['tap_name']
                    cmd.extend([
                        f"-netdev tap,id={tap_name},ifname={tap_name},script=no,downscript=no",
                        f"-device e1000,netdev={tap_name},mac={interface['mac_address']}"
                    ])
                    Logger.debug(f"Interface: {tap_name}, MAC: {interface['mac_address']}")

                # Ejecutar QEMU
                Logger.info("Ejecutando QEMU...")
                Logger.debug(f"Comando: {' '.join(cmd)}")
                ssh.execute(' '.join(cmd))
                
                # Verificar proceso
                Logger.info("Verificando proceso QEMU...")
                time.sleep(2)
                max_attempts = 3
                qemu_pid = None
                
                for attempt in range(max_attempts):
                    try:
                        stdout, _ = ssh.execute(
                            f"pgrep -f 'guest=VM{vm_info['id']}-S{slice_id}'"
                        )
                        if stdout.strip():
                            qemu_pid = int(stdout.strip())
                            break
                    except:
                        pass
                    time.sleep(1)

                if not qemu_pid:
                    stdout, _ = ssh.execute("tail -n 50 /tmp/qemu.log")
                    raise Exception(f"No se pudo obtener PID de QEMU. Logs:\n{stdout}")

                # Verificar estado
                stdout, _ = ssh.execute(f"ps -p {qemu_pid} -o state=")
                if not stdout.strip() or stdout.strip() not in ['R', 'S']:
                    raise Exception(f"Proceso QEMU {qemu_pid} no está corriendo correctamente")

                Logger.success(f"VM ID-{vm_info['id']} iniciada con PID {qemu_pid}")
                return True, qemu_pid, vnc_display

        except Exception as e:
            Logger.error(f"Error reanudando VM: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, 0, 0

    def restart_vm(self, request_data: dict) -> Tuple[bool, int, int]:
        """
        Reinicia una VM específica mediante pausa y reanudación.

        Realiza el proceso completo de reinicio:
        1. Pausa la VM deteniendo su proceso QEMU actual
        2. Espera a que se complete la detención
        3. Reanuda la VM con un nuevo proceso QEMU
        4. Verifica el estado final

        Args:
            request_data (dict): Datos necesarios con:
                - vm_info: Información de la VM
                - worker_info: Información del worker
                - slice_id: ID del slice

        Returns:
            Tuple[bool, int, int]: (éxito, PID de QEMU, número de display VNC)
                - éxito: True si el reinicio fue exitoso
                - PID: Process ID del nuevo proceso QEMU
                - display: Número de display VNC asignado

        Raises:
            Exception: Si hay errores en la pausa o reanudación
        """
        try:
            Logger.section(f"REINICIANDO VM ID-{request_data['vm_info']['id']}")
            Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

            # 1. Pausar la VM
            Logger.info("Pausando VM...")
            if not self.pause_vm(request_data):
                raise Exception("Error durante la pausa de la VM")
            Logger.success("VM pausada exitosamente")

            # 2. Esperar a que se complete la detención
            Logger.info("Esperando detención completa...")
            time.sleep(1)

            # 3. Reanudar la VM
            Logger.info("Reanudando VM...")
            success, qemu_pid, vnc_display = self.resume_vm(request_data)
            if not success:
                raise Exception("Error durante la reanudación de la VM")

            Logger.success(f"VM ID-{request_data['vm_info']['id']} reiniciada exitosamente (PID: {qemu_pid}, Display: {vnc_display})")
            return True, qemu_pid, vnc_display

        except Exception as e:
            Logger.error(f"Error reiniciando VM ID-{request_data['vm_info']['id']}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, 0, 0

    def _cleanup_deployment_state(self, slice_config: dict, current_stage: str = None):
        """
        Limpia los recursos creados durante un despliegue fallido.
        """
        with cleanup_lock:
            try:
                Logger.section("INICIANDO LIMPIEZA DE RECURSOS")

                # Obtener configuración necesaria
                network_config = slice_config['network_config']
                svlan = network_config['svlan_id']
                network = network_config['network']
                slice_bridge = network_config['slice_bridge_name']
                patch_slice = network_config['patch_ports']['slice_side']
                patch_int = network_config['patch_ports']['int_side']
                dhcp_if = network_config['dhcp_interface']
                gateway_if = network_config['gateway_interface']

                # Orden de limpieza (de más crítico a menos crítico)
                deployment_order = [
                    'worker_setup',   # Configuración completa
                    'images',         # Imágenes base
                    'network',        # Red y DHCP
                    'init'           # VNC ports
                ]

                # Determinar etapas a limpiar
                if current_stage:
                    try:
                        start_index = deployment_order.index(current_stage)
                        stages_to_clean = deployment_order[start_index:]  # Desde la etapa actual hasta el final
                        Logger.info(f"Limpiando desde {current_stage} hasta init")
                    except ValueError:
                        Logger.warning(f"Etapa '{current_stage}' no reconocida, limpiando todo")
                        stages_to_clean = deployment_order
                else:
                    stages_to_clean = deployment_order

                Logger.info(f"Etapas a limpiar: {stages_to_clean}")

                def clean_worker(worker_info: dict, vms: list, results: dict):
                    try:
                        # Verificar worker_info
                        if not isinstance(worker_info, dict):
                            raise ValueError("Invalid worker_info format")

                        worker_id = str(worker_info['id'])
                        full_worker_info = slice_config['resources_info']['workers'].get(worker_id)
                        if not full_worker_info:
                            raise ValueError(f"Worker information not found for ID {worker_id}")

                        # Crear SSH info
                        ssh_info = {
                            'name': full_worker_info['name'],
                            'ip': full_worker_info['ip'],
                            'ssh_username': full_worker_info['ssh_username'],
                            'ssh_password': full_worker_info.get('ssh_password'),
                            'ssh_key_path': full_worker_info.get('ssh_key_path')
                        }

                        with SSHManager(ssh_info) as ssh:
                            # 1. Matar procesos QEMU
                            for vm in vms:
                                try:
                                    Logger.info(f"Matando proceso QEMU de VM ID-{vm['id']}...")
                                    # Verificar si el proceso existe antes de matarlo
                                    check_cmd = f"pgrep -f 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'"
                                    try:
                                        stdout, _ = ssh.execute(check_cmd)
                                        if stdout.strip():
                                            # Proceso existe, matarlo
                                            pids = stdout.strip().split('\n')
                                            for pid in pids:
                                                ssh.execute(f"sudo kill -9 {pid}")
                                            Logger.debug(f"Procesos QEMU eliminados: {pids}")
                                        else:
                                            Logger.debug(f"No se encontraron procesos QEMU para VM ID-{vm['id']}")
                                    except Exception as inner_e:
                                        # Si falla la verificación, intentar pkill de todas formas
                                        ssh.execute(
                                            f"sudo pkill -9 -f 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}' 2>/dev/null || true")
                                except Exception as e:
                                    # Solo loggear como warning si es un error real
                                    if "status=-1" not in str(e) and "No such process" not in str(e):
                                        Logger.warning(f"Error matando proceso de VM ID-{vm['id']}: {str(e)}")
                                    else:
                                        Logger.debug(f"VM ID-{vm['id']} no tenía proceso corriendo")

                            time.sleep(2)

                            # 2. Limpiar imágenes
                            try:
                                Logger.info("Limpiando imágenes...")
                                ssh.execute("sudo rm -f /home/ubuntu/SliceManager/images/vm-*-slice-*.qcow2")
                            except Exception as e:
                                Logger.warning(f"Error limpiando imágenes: {str(e)}")

                            # 3. Limpiar interfaces y bridges
                            cleanup_commands = []
                            for vm in vms:
                                vm_interfaces = [i for i in slice_config['topology_info']['interfaces']
                                            if i['vm_id'] == vm['id']]

                                for interface in vm_interfaces:
                                    tap_name = interface.get('tap_name')
                                    if tap_name:
                                        if interface.get('external_access'):
                                            cleanup_commands.append(f"sudo ovs-vsctl --if-exists del-port br-int {tap_name}")
                                        else:
                                            cleanup_commands.append(f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {tap_name}")
                                        cleanup_commands.extend([
                                            f"sudo ip link set {tap_name} down 2>/dev/null || true",
                                            f"sudo ip tuntap del {tap_name} mode tap 2>/dev/null || true"
                                        ])

                            # 4. Limpiar bridges y patch ports
                            cleanup_commands.extend([
                                f"sudo ovs-vsctl --if-exists del-port br-int {patch_int}",
                                f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {patch_slice}",
                                f"sudo ovs-vsctl --if-exists del-br {slice_bridge}"
                            ])

                            # Ejecutar comandos
                            if cleanup_commands:
                                try:
                                    ssh.execute(" && ".join(cleanup_commands))
                                except Exception as e:
                                    Logger.warning(f"Error en comandos de limpieza: {str(e)}")

                            results[full_worker_info['name']] = {'success': True, 'error': None}

                    except Exception as e:
                        worker_name = worker_info.get('name', 'unknown')
                        results[worker_name] = {'success': False, 'error': str(e)}

                # Procesar etapas de limpieza
                for stage in stages_to_clean:
                    Logger.subsection(f"LIMPIANDO ETAPA: {stage}")

                    if stage == 'network' or stage == 'init':
                        Logger.info("Limpiando configuración inicial de red y DHCP...")
                        try:
                            # Obtener identificadores únicos del slice
                            slice_id = slice_config['slice_info']['id']
                            namespace_name = f"ns-slice-{slice_id}"
                            veth_int_name = f"veth-int-{slice_id}"
                            veth_ext_name = f"veth-ext-{slice_id}"

                            # Matar dnsmasq específico del slice
                            cmd = f"pgrep -f 'dnsmasq.{svlan}.conf'"
                            try:
                                pid = subprocess.check_output(cmd, shell=True).decode().strip()
                                if pid:
                                    Logger.info(f"Matando proceso dnsmasq (PID: {pid})")
                                    os.system(f"sudo kill -9 {pid}")
                            except subprocess.CalledProcessError:
                                Logger.debug("No se encontró proceso dnsmasq activo")

                            # Eliminar interfaces específicas del slice
                            Logger.info("Eliminando interfaces específicas del slice...")
                            ovs_cleanup_commands = [
                                f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {veth_ext_name}",
                                f"sudo ovs-vsctl --if-exists del-port {NETWORK_CONFIG['headnode_br']} {gateway_if}"
                            ]
                            for cmd in ovs_cleanup_commands:
                                os.system(cmd)

                            # Limpiar namespace específico del slice
                            Logger.info(f"Eliminando namespace {namespace_name}...")
                            ns_cleanup_commands = [
                                f"sudo ip netns exec {namespace_name} ip link del veth-int.{svlan} 2>/dev/null || true",
                                f"sudo ip netns del {namespace_name} 2>/dev/null || true"
                            ]
                            for cmd in ns_cleanup_commands:
                                os.system(cmd)

                            # Eliminar interfaces del host
                            Logger.info("Eliminando interfaces del host...")
                            host_cleanup_commands = [
                                f"sudo ip link del {gateway_if} 2>/dev/null || true",
                                f"sudo ip link del {veth_ext_name} 2>/dev/null || true"
                            ]
                            for cmd in host_cleanup_commands:
                                os.system(cmd)

                            # Limpiar namespace específico del slice
                            Logger.info(f"Eliminando namespace {namespace_name}...")
                            ns_cleanup_commands = [
                                f"sudo ip netns exec {namespace_name} ip link del veth-int.{svlan} 2>/dev/null || true",
                                f"sudo ip netns del {namespace_name} 2>/dev/null || true"
                            ]
                            for cmd in ns_cleanup_commands:
                                os.system(cmd)

                            # Limpiar reglas de iptables y forwarding específicas del slice
                            Logger.info("Limpiando reglas de red específicas...")
                            cleanup_commands = [
                                f"sudo iptables -t nat -D POSTROUTING -s {network} -o {NETWORK_CONFIG['internet_interface']} -j MASQUERADE 2>/dev/null || true",
                                f"sudo iptables -D FORWARD -i {gateway_if} -o {NETWORK_CONFIG['internet_interface']} -s {network} -j ACCEPT 2>/dev/null || true",
                                f"sudo iptables -D FORWARD -i {NETWORK_CONFIG['internet_interface']} -o {gateway_if} -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null || true",
                                "sudo sysctl -w net.ipv4.ip_forward=1"
                            ]
                            for cmd in cleanup_commands:
                                os.system(cmd)

                            # Limpiar configuración DHCP específica del slice
                            Logger.info("Limpieza final de configuración DHCP...")
                            dhcp_config = os.path.join(DATA_DIR['dhcp']['config'], f'dnsmasq.{svlan}.conf')
                            if os.path.exists(dhcp_config):
                                os.remove(dhcp_config)

                        except Exception as e:
                            Logger.error(f"Error limpiando red: {str(e)}")

                    elif stage in ['images', 'worker_setup']:
                        Logger.info("Limpiando workers en paralelo...")
                        # Agrupar VMs por worker
                        vms_by_worker = {}
                        for vm in slice_config['topology_info']['vms']:
                            worker_id = str(vm['physical_server']['id'])
                            worker_info = slice_config['resources_info']['workers'][worker_id]
                            if worker_info['name'] not in vms_by_worker:
                                vms_by_worker[worker_info['name']] = {
                                    'info': worker_info,
                                    'vms': []
                                }
                            vms_by_worker[worker_info['name']]['vms'].append(vm)

                        # Limpiar workers en paralelo
                        threads = []
                        worker_results = {}

                        for worker_name, worker_data in vms_by_worker.items():
                            thread = threading.Thread(
                                target=clean_worker,
                                args=(worker_data['info'], worker_data['vms'], worker_results)
                            )
                            threads.append(thread)
                            thread.start()

                        # Esperar threads y verificar resultados
                        for thread in threads:
                            thread.join()

                        failed_workers = [
                            worker for worker, result in worker_results.items()
                            if not result['success']
                        ]

                        if failed_workers:
                            error_messages = [
                                f"{worker}: {worker_results[worker]['error']}"
                                for worker in failed_workers
                            ]
                            Logger.error("Errores durante la limpieza:")
                            for error in error_messages:
                                Logger.error(f"  - {error}")

                Logger.success(f"Limpieza completada para slice {slice_config['slice_info']['id']}")

            except Exception as e:
                Logger.error(f"Error durante limpieza: {str(e)}")
                Logger.debug(f"Traceback: {traceback.format_exc()}")
                raise

class ImageManager:
    """
    Gestor de sincronización de imágenes entre headnode y workers.

    Esta clase maneja:
    - Verificación y cálculo de hashes de imágenes
    - Sincronización de imágenes base entre nodos
    - Gestión de directorios y permisos en workers
    - Transferencia segura vía SFTP

    Attributes:
        headnode_images_dir (str): Directorio de imágenes base en headnode
        worker_base_path (str): Ruta base en workers
        worker_base_dir (str): Directorio para imágenes base en workers
        worker_vm_dir (str): Directorio para imágenes de VM en workers
    """

    def __init__(self):
        """Inicializa las rutas de directorios para imágenes."""
        self.headnode_images_dir = './images'
        self.worker_base_path = '/home/ubuntu/SliceManager'
        self.worker_base_dir = '/home/ubuntu/SliceManager/base'
        self.worker_vm_dir = '/home/ubuntu/SliceManager/images'
        Logger.debug("ImageManager inicializado")
        
    def get_image_hash(self, ssh: SSHManager, image_path: str) -> str:
        """
        Calcula el hash SHA256 de una imagen en un worker remoto.
        
        Args:
            ssh (SSHManager): Conexión SSH al worker
            image_path (str): Ruta completa a la imagen
            
        Returns:
            str: Hash SHA256 de la imagen, o None si hay error
        """
        try:
            Logger.debug(f"Calculando hash de {image_path}")
            stdout, _ = ssh.execute(f"sha256sum {image_path}")
            hash_value = stdout.split()[0] if stdout else None
            Logger.debug(f"Hash calculado: {hash_value}")
            return hash_value
        except Exception as e:
            Logger.warning(f"Error calculando hash: {str(e)}")
            return None
            
    def get_local_image_hash(self, image_path: str) -> str:
        """
        Calcula el hash SHA256 de una imagen local en el headnode.
        
        Args:
            image_path (str): Ruta a la imagen local
            
        Returns:
            str: Hash SHA256 de la imagen
            
        Raises:
            Exception: Si hay error leyendo la imagen
        """
        try:
            Logger.debug(f"Calculando hash local de {image_path}")
            import hashlib
            sha256_hash = hashlib.sha256()
            
            with open(image_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
                    
            hash_value = sha256_hash.hexdigest()
            Logger.debug(f"Hash calculado: {hash_value}")
            return hash_value
        except Exception as e:
            Logger.error(f"Error calculando hash local: {str(e)}")
            raise
            
    def check_image_in_worker(self, worker_info: dict, image_path: str) -> bool:
        """
        Verifica si una imagen base existe en un worker y su hash coincide.
        
        Args:
            worker_info (dict): Información del worker con credenciales SSH
            image_path (str): Ruta a la imagen base en el headnode
            
        Returns:
            bool: True si la imagen existe y es válida
        """
        try:
            Logger.subsection(f"VERIFICANDO IMAGEN EN {worker_info['name']}")
            Logger.debug(f"Imagen: {os.path.basename(image_path)}")

            ssh_info = {
                'hostname': worker_info['name'],
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            with SSHManager(ssh_info) as ssh:
                base_name = os.path.basename(image_path)
                worker_base_path = f"{self.worker_base_dir}/{base_name}"
                
                # Verificar existencia
                stdout, _ = ssh.execute(f"test -f {worker_base_path} && echo 'exists'")
                if 'exists' not in stdout:
                    Logger.info(f"Imagen no encontrada en {worker_info['name']}")
                    return False

                Logger.success(f"Imagen encontrada en {worker_info['name']}")
                return True
                    
        except Exception as e:
            Logger.error(f"Error verificando imagen en {worker_info['name']}: {str(e)}")
            return False

    def sync_image_to_worker(self, worker_info: dict, image_path: str) -> bool:
        """
        Sincroniza una imagen base con un worker específico.
        
        Args:
            worker_info (dict): Información del worker destino
            image_path (str): Ruta a la imagen a sincronizar
            
        Returns:
            bool: True si la sincronización fue exitosa
        """
        try:
            Logger.subsection(f"SINCRONIZANDO IMAGEN CON {worker_info['name']}")
            Logger.debug(f"Imagen: {os.path.basename(image_path)}")

            # Verificar si ya existe
            if self.check_image_in_worker(worker_info, image_path):
                Logger.info(f"Imagen ya existe en {worker_info['name']}")
                return True
                
            Logger.info("Iniciando transferencia...")
            
            ssh_info = {
                'hostname': worker_info['name'],
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            with SSHManager(ssh_info) as ssh:
                # Preparar directorios
                Logger.info("Configurando directorios...")
                ssh.execute(f"sudo mkdir -p {self.worker_base_path}")
                ssh.execute(f"sudo mkdir -p {self.worker_base_dir} {self.worker_vm_dir}")
                ssh.execute(f"sudo chmod 777 {self.worker_base_dir} {self.worker_vm_dir}")
                
                # Transferir imagen
                Logger.info("Transfiriendo imagen...")
                sftp = ssh.client.open_sftp()
                sftp.put(image_path, f"{self.worker_base_dir}/{os.path.basename(image_path)}")
                sftp.close()
                
                # Restaurar permisos
                Logger.info("Restaurando permisos...")
                ssh.execute(f"sudo chmod 755 {self.worker_base_dir} {self.worker_vm_dir}")
                ssh.execute(f"sudo chown -R ubuntu:ubuntu {self.worker_base_dir} {self.worker_vm_dir}")
                
            Logger.success(f"Imagen sincronizada exitosamente con {worker_info['name']}")
            return True
                
        except Exception as e:
            Logger.error(f"Error sincronizando con {worker_info['name']}: {str(e)}")
            return False
        
    def sync_all_images_to_workers(self) -> dict:
        """
        Sincroniza todas las imágenes base con todos los workers.
        
        Returns:
            dict: Resultados de sincronización por worker:
                {worker_name: {'success': bool, 'errors': [str]}}
        """
        Logger.section("SINCRONIZANDO IMÁGENES CON WORKERS")
        results = {}
        
        # Obtener workers
        workers = [node for node in NODES.keys() if node != 'ofs']
        Logger.info(f"Workers a procesar: {workers}")
        
        # Inicializar resultados
        for worker in workers:
            results[worker] = {
                'success': True,
                'errors': []
            }
        
        # Procesar cada worker
        for worker in workers:
            Logger.subsection(f"SINCRONIZANDO {worker}")
            
            try:
                with SSHManager(worker) as ssh:
                    # Preparar directorios
                    Logger.info("Configurando directorios...")
                    ssh.execute(f"sudo mkdir -p {self.worker_base_dir} {self.worker_vm_dir}")
                    ssh.execute(f"sudo chmod 777 {self.worker_base_dir} {self.worker_vm_dir}")
                    
                    # Procesar imágenes
                    for image in AVAILABLE_IMAGES:
                        try:
                            image_path = image['path']
                            image_name = os.path.basename(image_path)
                            Logger.info(f"Procesando {image_name}")
                            
                            if self.check_image_in_worker(worker, image_path):
                                Logger.debug(f"Imagen {image_name} ya existe")
                                continue
                                
                            Logger.info(f"Copiando {image_name}...")
                            sftp = ssh.client.open_sftp()
                            sftp.put(image_path, f"{self.worker_base_dir}/{image_name}")
                            sftp.close()
                            Logger.success(f"Imagen {image_name} copiada")
                            
                        except Exception as e:
                            error_msg = f"Error sincronizando {image_name}: {str(e)}"
                            Logger.error(error_msg)
                            results[worker]['errors'].append(error_msg)
                            results[worker]['success'] = False
                    
                    # Restaurar permisos
                    Logger.info("Restaurando permisos...")
                    ssh.execute(f"sudo chmod 755 {self.worker_base_dir} {self.worker_vm_dir}")
                    ssh.execute(f"sudo chown -R ubuntu:ubuntu {self.worker_base_dir} {self.worker_vm_dir}")
                    
            except Exception as e:
                error_msg = f"Error general con {worker}: {str(e)}"
                Logger.error(error_msg)
                results[worker]['errors'].append(error_msg)
                results[worker]['success'] = False
                
        # Resumen
        Logger.section("RESUMEN DE SINCRONIZACIÓN")
        for worker, result in results.items():
            status = "✓ OK" if result['success'] else "✗ ERROR"
            Logger.info(f"{worker}: {status}")
            if result['errors']:
                for error in result['errors']:
                    Logger.error(f"  - {error}")
        
        return results

class DatabaseManager:
    """
    Gestor de conexiones y operaciones con la base de datos MySQL.

    Esta clase maneja un pool de conexiones y proporciona métodos para:
    - Ejecutar queries individuales
    - Ejecutar transacciones con múltiples queries
    - Obtener resultados en formato de diccionario

    Atributos:
        cnx_pool: Pool de conexiones MySQL configurado con POOL_CONFIG
    """

    def __init__(self):
        """
        Inicializa el gestor creando el pool de conexiones MySQL.
        """
        Logger.info("Iniciando DatabaseManager")
        try:
            self.cnx_pool = mysql.connector.pooling.MySQLConnectionPool(**POOL_CONFIG)
            Logger.success("Pool de conexiones MySQL creado exitosamente")
            Logger.debug(f"Configuración del pool: {POOL_CONFIG}")
        except Exception as e:
            Logger.error(f"Error creando pool de conexiones: {str(e)}")
            raise

    def get_connection(self):
        """
        Obtiene una conexión del pool.

        Returns:
            MySQLConnection: Una conexión activa del pool

        Raises:
            PoolError: Si no hay conexiones disponibles en el pool
        """
        try:
            conn = self.cnx_pool.get_connection()
            Logger.debug("Conexión obtenida del pool")
            return conn
        except Exception as e:
            Logger.error(f"Error obteniendo conexión del pool: {str(e)}")
            raise

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Ejecuta una query SQL y retorna los resultados.

        Args:
            query (str): Query SQL a ejecutar
            params (tuple, optional): Parámetros para la query

        Returns:
            List[Dict]: Lista de resultados donde cada fila es un diccionario

        Raises:
            Exception: Si hay error ejecutando la query
        """
        connection = None
        cursor = None
        try:
            Logger.debug(f"Ejecutando query: {query.strip()}")
            if params:
                Logger.debug(f"Parámetros: {params}")

            connection = self.get_connection()
            cursor = connection.cursor(dictionary=True)

            cursor.execute(query, params)

            if cursor.with_rows:
                results = cursor.fetchall()
                Logger.debug(f"Filas obtenidas: {len(results)}")
                return results
            else:
                # Para queries de UPDATE/INSERT, verificar cantidad de filas afectadas
                affected_rows = cursor.rowcount
                Logger.debug(f"Query ejecutada sin resultados, filas afectadas: {affected_rows}")

                # Hacer commit explícito para queries de modificación
                connection.commit()

                return []

        except mysql.connector.Error as e:
            Logger.error(f"Error de MySQL ejecutando query: {str(e)}")
            Logger.error(f"Código de error: {e.errno}")
            Logger.error(f"Mensaje SQL: {e.msg}")
            Logger.debug(f"Query: {query}")
            Logger.debug(f"Parámetros: {params}")

            # Hacer rollback en caso de error
            if connection:
                connection.rollback()
            raise

        except Exception as e:
            Logger.error(f"Error general ejecutando query: {str(e)}")
            Logger.debug(f"Query: {query}")
            Logger.debug(f"Parámetros: {params}")

            # Hacer rollback en caso de error
            if connection:
                connection.rollback()
            raise

        finally:
            # Cerrar cursor y conexión
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def execute_transaction(self, queries: List[Tuple[str, tuple]]) -> bool:
        """
        Ejecuta múltiples queries en una transacción atómica.

        Args:
            queries (List[Tuple[str, tuple]]): Lista de tuplas (query, params)

        Returns:
            bool: True si la transacción fue exitosa

        Raises:
            Exception: Si hay error en la transacción (se hace rollback)
        """
        try:
            Logger.debug(f"Iniciando transacción con {len(queries)} queries")

            with self.get_connection() as cnx:
                with cnx.cursor() as cursor:
                    for i, (query, params) in enumerate(queries, 1):
                        Logger.debug(f"Ejecutando query {i}/{len(queries)}")
                        Logger.debug(f"Query: {query}")
                        Logger.debug(f"Parámetros: {params}")
                        cursor.execute(query, params)

                Logger.debug("Haciendo commit de la transacción")
                cnx.commit()
                Logger.success("Transacción completada exitosamente")
                return True

        except Exception as e:
            Logger.error(f"Error en transacción: {str(e)}")
            Logger.warning("Ejecutando rollback")
            if 'cnx' in locals():
                cnx.rollback()
            raise

class KafkaProcessor:
    """
    Procesador de mensajes Kafka para operaciones del Linux Driver.

    Maneja:
    - Consumo de múltiples tópicos con prioridades
    - Procesamiento paralelo por partición
    - Garantía de orden FIFO por SliceId
    - Manejo de errores y reintentos
    - Notificación de estado de operaciones

    Attributes:
        slice_manager: Instancia de SliceManager
        db_manager: Instancia de DatabaseManager
        consumers: Diccionario de consumidores por prioridad
        worker_threads: Diccionario de hilos trabajadores
    """

    def __init__(self):
        Logger.major_section("INICIANDO PROCESADOR KAFKA")
        self.slice_manager = SliceManager()
        self.db_manager = DatabaseManager()
        self.consumers = {}
        self.worker_threads = {}
        self.topic_partitions = {}
        self.message_queues = {}
        self.exit_flag = threading.Event()

        # Lock para operaciones de BD
        self.db_lock = threading.RLock()

        # Set para tracking de mensajes procesados (evitar duplicados)
        self.processed_messages = set()
        self.processed_lock = threading.RLock()

        self._init_db()
        Logger.success("Procesador Kafka inicializado")

    def _init_db(self):
        """Verifica la conexión a la base de datos y la disponibilidad de las tablas."""
        try:
            Logger.info("Verificando conexión a la base de datos...")

            # Verificar que podemos acceder a la tabla operation_requests
            check_query = """
                          SELECT COUNT(*) as count
                          FROM information_schema.tables
                          WHERE table_schema = %s
                            AND table_name IN ('operation_requests', 'queue_metrics')
                          """

            results = self.db_manager.execute_query(check_query, (DB_CONFIG['database'],))

            if results and results[0]['count'] == 2:
                Logger.success("Tablas operation_requests y queue_metrics verificadas correctamente")
            else:
                Logger.warning("No se encontraron todas las tablas necesarias en la base de datos")
                Logger.debug(f"Resultado de verificación: {results}")

            # Actualizar métricas iniciales
            self._update_queue_metrics()

        except Exception as e:
            Logger.error(f"Error verificando base de datos: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def start(self):
        """
        Inicia los consumidores Kafka y los hilos trabajadores.
        """
        try:
            Logger.section("INICIANDO CONSUMIDORES KAFKA")

            # Iniciar consumidores por prioridad
            for priority, config in KAFKA_TOPICS.items():
                Logger.info(f"Iniciando consumidor para tópico {config['name']} ({priority})")

                # Crear colas para cada partición
                self.message_queues[priority] = [queue.Queue() for _ in range(config['partitions'])]

                # Iniciar consumidor
                consumer_thread = threading.Thread(
                    target=self._consumer_thread,
                    args=(priority, config),
                    name=f"kafka-consumer-{priority.lower()}"
                )
                consumer_thread.daemon = True
                consumer_thread.start()
                self.consumers[priority] = consumer_thread

                # Iniciar trabajadores para cada partición
                self.worker_threads[priority] = []
                for partition in range(config['partitions']):
                    worker_thread = threading.Thread(
                        target=self._worker_thread,
                        args=(priority, partition, self.message_queues[priority][partition]),
                        name=f"worker-{priority.lower()}-p{partition}"
                    )
                    worker_thread.daemon = True
                    worker_thread.start()
                    self.worker_threads[priority].append(worker_thread)

                Logger.success(f"Consumidor {priority} iniciado con {config['partitions']} particiones")

            Logger.success("Todos los consumidores Kafka iniciados correctamente")

        except Exception as e:
            Logger.error(f"Error iniciando consumidores Kafka: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def stop(self):
        """
        Detiene consumidores y trabajadores de forma segura.
        """
        try:
            Logger.section("DETENIENDO PROCESADOR KAFKA")

            # Señalizar a los hilos que deben detenerse
            self.exit_flag.set()

            # Esperar a que terminen los hilos (timeout)
            timeout = 30  # segundos
            Logger.info(f"Esperando hasta {timeout} segundos para cierre ordenado...")

            for priority, threads in self.worker_threads.items():
                for idx, thread in enumerate(threads):
                    thread.join(timeout / len(threads))
                    if thread.is_alive():
                        Logger.warning(f"Trabajador {priority}-{idx} no finalizó a tiempo")

            for priority, thread in self.consumers.items():
                thread.join(timeout / len(self.consumers))
                if thread.is_alive():
                    Logger.warning(f"Consumidor {priority} no finalizó a tiempo")

            Logger.success("Procesador Kafka detenido")

        except Exception as e:
            Logger.error(f"Error deteniendo procesador Kafka: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")

    def _consumer_thread(self, priority, config):
        """
        Hilo de consumidor Kafka para un tópico específico.

        Args:
            priority: Nivel de prioridad (HIGH, MEDIUM, LOW)
            config: Configuración del tópico y particiones
        """
        consumer = None
        try:
            Logger.info(f"Iniciando hilo consumidor {priority}")

            # Configurar consumidor con configuración mejorada
            consumer_config = KAFKA_CONFIG.copy()
            consumer_config['group.id'] = f"{consumer_config['group.id']}-{priority.lower()}"

            consumer = Consumer(consumer_config)
            consumer.subscribe([config['name']])

            Logger.debug(f"Consumidor {priority} suscrito al tópico {config['name']}")

            # Bucle principal de consumo
            consecutive_errors = 0
            while not self.exit_flag.is_set():
                try:
                    # Poll para nuevos mensajes
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        consecutive_errors = 0
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            Logger.debug(f"Fin de partición {msg.partition()}")
                        else:
                            Logger.error(f"Error en consumidor {priority}: {msg.error()}")
                            consecutive_errors += 1
                            if consecutive_errors > 5:
                                time.sleep(5)
                                consecutive_errors = 0
                        continue

                    # Procesar mensaje
                    try:
                        partition = msg.partition()
                        offset = msg.offset()
                        payload = json.loads(msg.value().decode('utf-8'))
                        operation_id = payload.get('id')

                        # Crear identificador único del mensaje
                        message_id = f"{config['name']}-{partition}-{offset}-{operation_id}"

                        # Verificar si ya procesamos este mensaje
                        with self.processed_lock:
                            if message_id in self.processed_messages:
                                Logger.warning(f"Mensaje duplicado detectado: {message_id}, ignorando...")
                                consumer.commit(msg)
                                continue
                            else:
                                self.processed_messages.add(message_id)

                        # Agregar metadatos para tracking
                        payload['_kafka_metadata'] = {
                            'topic': config['name'],
                            'partition': partition,
                            'offset': offset,
                            'message_id': message_id
                        }

                        # Enrutar mensaje a su cola de partición
                        self.message_queues[priority][partition].put(payload)
                        Logger.debug(f"Mensaje {operation_id} encolado en partición {partition}")

                        # Commit exitoso
                        consumer.commit(msg)
                        consecutive_errors = 0

                    except json.JSONDecodeError:
                        Logger.error(f"Mensaje con formato inválido: {msg.value()}")
                        consumer.commit(msg)
                    except Exception as queue_error:
                        Logger.error(f"Error encolando mensaje en partición {partition}: {str(queue_error)}")
                        time.sleep(0.1)

                except Exception as e:
                    consecutive_errors += 1
                    if not self.exit_flag.is_set():
                        Logger.error(f"Error en bucle de consumo {priority}: {str(e)}")
                        if consecutive_errors > 10:
                            break
                        time.sleep(min(consecutive_errors * 0.5, 5))

        except Exception as e:
            Logger.error(f"Error fatal en consumidor {priority}: {str(e)}")
        finally:
            if consumer:
                try:
                    consumer.close()
                    Logger.debug(f"Consumidor {priority} cerrado correctamente")
                except Exception as e:
                    Logger.warning(f"Error cerrando consumidor {priority}: {str(e)}")

    def _worker_thread(self, priority, partition, message_queue):
        """
        Hilo trabajador para procesar mensajes de una partición específica.

        Args:
            priority: Nivel de prioridad (HIGH, MEDIUM, LOW)
            partition: Número de partición
            message_queue: Cola de mensajes para esta partición
        """
        thread_name = f"{priority}-P{partition}"
        Logger.info(f"Iniciando hilo trabajador {thread_name}")

        # Procesamiento de mensajes
        while not self.exit_flag.is_set():
            try:
                # Obtener mensaje (con timeout para permitir verificación de exit_flag)
                try:
                    message = message_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                operation_id = message.get('id')
                slice_id = message.get('payload', {}).get('slice_info', {}).get('id')
                operation_type = message.get('operationType')

                Logger.subsection(f"PROCESANDO OPERACIÓN {operation_id} (Slice {slice_id})")
                Logger.debug(f"🔍 Tipo: {operation_type}, Prioridad: {priority}, Partición: {partition}")
                message_metadata = message.get('_kafka_metadata', {})
                Logger.debug(f"🔍 Worker: {thread_name}, Metadata: {message_metadata}")

                # Solo actualizar estado una vez
                # Verificar orden de operaciones para el mismo slice
                if slice_id is not None:
                    self._process_in_order(message, priority, partition, thread_name)
                else:
                    # Solo para operaciones sin slice_id
                    self._update_operation_status(message, "PROCESSING")
                    self._process_operation(message)

                # Marcar mensaje como completado
                message_queue.task_done()

            except Exception as e:
                if not self.exit_flag.is_set():
                    Logger.error(f"Error en trabajador {thread_name}: {str(e)}")
                    Logger.debug(f"Traceback: {traceback.format_exc()}")
                    # Actualizar estado de error
                    try:
                        self._update_operation_status(message, "ERROR", error_message=str(e))
                    except:
                        pass
                    # Pequeña pausa para prevenir ciclos de error
                    time.sleep(0.5)

        Logger.debug(f"Hilo trabajador {thread_name} finalizado")

    def _process_in_order(self, message, priority, partition, thread_name):
        """
        Procesa operaciones manteniendo orden FIFO por SliceId.

        Args:
            message: Mensaje a procesar
            priority: Nivel de prioridad
            partition: Número de partición
        """
        slice_id = message.get('payload', {}).get('slice_info', {}).get('id')

        if not slice_id:
            # Si no hay SliceId, procesar directamente
            self._update_operation_status(message, "PROCESSING")
            self._process_operation(message)
            return

        # Adquirir lock para operaciones de este slice
        with slice_queue_lock:
            if slice_id not in slice_operation_queues:
                slice_operation_queues[slice_id] = queue.Queue()
                slice_operation_locks[slice_id] = threading.Lock()

        # Añadir operación a la cola de este slice
        slice_operation_queues[slice_id].put((message, thread_name))

        # Intentar procesar cola de este slice (solo lo hará el primer thread que tome el lock)
        if slice_operation_locks[slice_id].acquire(blocking=False):
            try:
                # Procesar todas las operaciones pendientes en orden
                while not slice_operation_queues[slice_id].empty():
                    try:
                        next_operation, origin_thread = slice_operation_queues[slice_id].get(block=False)
                        operation_id = next_operation.get('id')

                        Logger.debug(
                            f"🔍 Procesando operación {operation_id} (origen: {origin_thread}, procesador: {thread_name})")

                        # CORRECCIÓN: Solo actualizar estado a PROCESSING para la primera operación
                        # y solo por el thread que realmente procesará
                        if origin_thread == thread_name:
                            self._update_operation_status(next_operation, "PROCESSING")

                        self._process_operation(next_operation)
                        slice_operation_queues[slice_id].task_done()

                    except queue.Empty:
                        break
                    except Exception as e:
                        Logger.error(f"Error procesando operación para slice {slice_id}: {str(e)}")
                        self._update_operation_status(next_operation, "ERROR", error_message=str(e))
                        slice_operation_queues[slice_id].task_done()
            finally:
                # Liberar lock
                slice_operation_locks[slice_id].release()

                # Limpiar recursos si la cola está vacía
                with slice_queue_lock:
                    if slice_operation_queues[slice_id].empty():
                        Logger.debug(f"🔍 Limpiando recursos para slice {slice_id}")
                        del slice_operation_queues[slice_id]
                        del slice_operation_locks[slice_id]

    def _process_operation(self, message):
        """
        Procesa una operación según su tipo.

        Args:
            message: Mensaje con la operación a procesar
        """
        try:
            operation_type = message.get('operationType')
            operation_id = message.get('id')

            Logger.info(f"ℹ Procesando operación {operation_id} de tipo {operation_type}")

            # Procesar según tipo de operación
            if operation_type == "DEPLOY_SLICE":
                self._process_deploy_slice(message)
            else:
                Logger.warning(f"Tipo de operación no implementado: {operation_type}")
                self._update_operation_status(
                    message,
                    "ERROR",
                    error_message=f"Tipo de operación no implementado: {operation_type}"
                )

        except Exception as e:
            Logger.error(f"Error procesando operación {message.get('id')}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            self._update_operation_status(
                message,
                "ERROR",
                error_message=str(e)
            )
            raise

    def _process_deploy_slice(self, message):
        """
        Procesa una operación de despliegue de slice.

        Args:
            message: Mensaje con la operación de despliegue
        """
        try:
            Logger.subsection(f"PROCESANDO DEPLOY_SLICE {message.get('id')}")

            # Extraer payload
            payload = message.get('payload', {})
            if not payload:
                raise ValueError("Payload vacío en mensaje de despliegue")

            # Actualizar a IN_PROGRESS y notificar antes de empezar
            Logger.info("ℹ Actualizando estado a IN_PROGRESS...")
            self._update_operation_status(message, "PROCESSING")  # Esto se mapea a IN_PROGRESS en BD

            # Notificar al Slice Manager que empezó el procesamiento
            try:
                self._notify_slice_manager_progress(message, "IN_PROGRESS", "Iniciando despliegue de slice...")
            except Exception as e:
                Logger.warning(f"Error notificando progreso: {str(e)}")
                # No detenemos el despliegue por esto

            # Ejecutar despliegue usando la lógica existente
            Logger.info("ℹ Iniciando despliegue con SliceManager...")
            deployed_config = self.slice_manager.deploy_slice(payload)

            # Actualizar resultado en la base de datos ANTES del estado final
            Logger.debug(f"🔍 Actualizando resultado de operación {message.get('id')}")
            self._update_operation_result(message.get('id'), deployed_config)

            # Actualizar estado final en base de datos
            Logger.debug(f"🔍 Actualizando estado de operación {message.get('id')} a COMPLETED")
            self._update_operation_status(message, "COMPLETED")

            # Notificar al Slice Manager
            Logger.debug(f"🔍 Notificando resultado final al Slice Manager")
            self._notify_slice_manager(message, "COMPLETED", deployed_config)

            Logger.success(f"✓ Despliegue de slice {payload.get('slice_info', {}).get('id')} completado exitosamente")

        except Exception as e:
            Logger.error(f"Error en despliegue: {str(e)}")
            # Limpiar recursos si es necesario
            Logger.debug(f"🔍 Limpiando recursos para slice {payload.get('slice_info', {}).get('id', 'unknown')}")
            self._update_operation_status(message, "ERROR", error_message=str(e))
            self._notify_slice_manager(message, "FAILED", error_message=str(e))
            raise

    def _update_operation_status(self, message, status, error_message=None):
        """
        Actualiza el estado de una operación en la base de datos.

        Args:
            message: Mensaje de la operación
            status: Nuevo estado (PENDING, PROCESSING, COMPLETED, ERROR)
            error_message: Mensaje de error (opcional)
        """
        operation_id = message.get('id')

        if not operation_id:
            Logger.error("No se puede actualizar estado: operation_id no encontrado")
            return

        with self.db_lock:
            try:
                Logger.debug(f"🔍 Actualizando estado de operación {operation_id} a {status}")

                # Mapear estados correctamente
                status_mapping = {
                    "PENDING": "PENDING",
                    "PROCESSING": "IN_PROGRESS",  # Mapeo correcto
                    "COMPLETED": "COMPLETED",
                    "ERROR": "FAILED"
                }

                db_status = status_mapping.get(status, status)
                current_time = datetime.now()

                # Construir query de actualización dinámica
                update_fields = ["status = %s"]
                update_params = [db_status]

                # Actualizar campos adicionales según el estado
                if db_status == "IN_PROGRESS":
                    update_fields.append("started_at = %s")
                    update_params.append(current_time)

                if db_status in ["COMPLETED", "FAILED"]:
                    update_fields.append("completed_at = %s")
                    update_params.append(current_time)

                if error_message and db_status == "FAILED":
                    update_fields.append("error_message = %s")
                    update_params.append(error_message)

                # Construir query final
                update_query = f"""
                    UPDATE operation_requests 
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                update_params.append(operation_id)

                Logger.debug(f"🔍 Ejecutando query: {update_query.strip()}")
                Logger.debug(f"🔍 Parámetros: {tuple(update_params)}")

                # Ejecutar query y verificar que se actualizó
                result = self.db_manager.execute_query(update_query, tuple(update_params))

                # Verificar que la actualización fue exitosa
                check_query = "SELECT status, started_at, completed_at FROM operation_requests WHERE id = %s"
                check_result = self.db_manager.execute_query(check_query, (operation_id,))

                if check_result:
                    actual_status = check_result[0]['status']
                    Logger.debug(f"🔍 Estado actualizado verificado: {actual_status}")
                    if actual_status != db_status:
                        Logger.warning(f"Estado en BD ({actual_status}) no coincide con esperado ({db_status})")
                else:
                    Logger.error(f"No se encontró operation_request con ID {operation_id}")

                Logger.debug(f"🔍 Query ejecutada exitosamente")

                # Solo actualizar métricas cuando la operación está completa
                if db_status in ["COMPLETED", "FAILED"]:
                    # Ejecutar en thread separado para no bloquear
                    metrics_thread = threading.Thread(target=self._update_queue_metrics)
                    metrics_thread.daemon = True
                    metrics_thread.start()

            except Exception as e:
                Logger.error(f"Error actualizando estado en BD para operación {operation_id}: {str(e)}")
                Logger.debug(f"Traceback: {traceback.format_exc()}")

                # Intentar un rollback si estamos en transacción
                try:
                    # Log del estado actual para debugging
                    check_query = "SELECT * FROM operation_requests WHERE id = %s"
                    current_state = self.db_manager.execute_query(check_query, (operation_id,))
                    Logger.debug(f"Estado actual en BD: {current_state}")
                except:
                    pass

    def _update_operation_result(self, operation_id, result):
        with self.db_lock:
            try:
                Logger.debug(f"🔍 Actualizando resultado de operación {operation_id}")

                update_query = "UPDATE operation_requests SET result_json = %s WHERE id = %s"
                result_json = json.dumps(result)

                Logger.debug(f"🔍 Ejecutando query: {update_query}")
                Logger.debug(f"🔍 Parámetros: (resultado_json, {operation_id})")

                self.db_manager.execute_query(update_query, (result_json, operation_id))
                Logger.debug(f"🔍 Query ejecutada exitosamente")

            except Exception as e:
                Logger.error(f"Error actualizando resultado en BD: {str(e)}")
                Logger.debug(f"Traceback: {traceback.format_exc()}")

    def _update_queue_metrics(self):
        """
        Actualiza las métricas de las colas en la tabla queue_metrics.
        Registra conteos y tiempos promedio para monitoreo.
        """
        with self.db_lock:
            try:
                # Obtener timestamp actual para el registro
                current_time = datetime.now()
                date_only = current_time.strftime('%Y-%m-%d 00:00:00')

                # Para cada cola en el sistema
                for priority, config in KAFKA_TOPICS.items():
                    queue_name = config['name']

                    # Query corregida con SUM para obtener conteos correctos
                    stats_query = """
                                  SELECT 
                                      SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending_count,
                                      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as in_progress_count,
                                      SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_count,
                                      SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
                                      AVG(CASE 
                                          WHEN started_at IS NOT NULL AND submitted_at IS NOT NULL 
                                          THEN TIMESTAMPDIFF(SECOND, submitted_at, started_at)
                                          ELSE NULL 
                                      END) as avg_wait_time,
                                      AVG(CASE 
                                          WHEN completed_at IS NOT NULL AND started_at IS NOT NULL 
                                          THEN TIMESTAMPDIFF(SECOND, started_at, completed_at)
                                          ELSE NULL 
                                      END) as avg_processing_time,
                                      MAX(CASE 
                                          WHEN started_at IS NOT NULL AND submitted_at IS NOT NULL 
                                          THEN TIMESTAMPDIFF(SECOND, submitted_at, started_at)
                                          ELSE NULL 
                                      END) as max_wait_time,
                                      MAX(CASE 
                                          WHEN completed_at IS NOT NULL AND started_at IS NOT NULL 
                                          THEN TIMESTAMPDIFF(SECOND, started_at, completed_at)
                                          ELSE NULL 
                                      END) as max_processing_time
                                  FROM operation_requests
                                  WHERE queue_name = %s
                                  """

                    Logger.debug(f"🔍 Ejecutando query: {stats_query}")
                    Logger.debug(f"🔍 Parámetros: ('{queue_name}',)")
                    stats = self.db_manager.execute_query(stats_query, (queue_name,))
                    Logger.debug(f"🔍 Filas obtenidas: {len(stats)}")

                    if not stats or not stats[0]:
                        Logger.debug(f"No hay estadísticas disponibles para la cola {queue_name}")
                        continue

                    stat = stats[0]

                    # Verificar si ya existe un registro para hoy
                    check_query = """
                                  SELECT id 
                                  FROM queue_metrics
                                  WHERE queue_name = %s 
                                    AND DATE(record_date) = DATE(%s)
                                  """

                    Logger.debug(f"🔍 Ejecutando query: {check_query}")
                    Logger.debug(f"🔍 Parámetros: ('{queue_name}', {current_time})")
                    existing = self.db_manager.execute_query(check_query, (queue_name, current_time))
                    Logger.debug(f"🔍 Filas obtenidas: {len(existing)}")

                    if existing:
                        # Actualizar registro existente
                        update_query = """
                                       UPDATE queue_metrics 
                                       SET pending_count = %s,
                                           in_progress_count = %s,
                                           completed_count = %s,
                                           failed_count = %s,
                                           average_wait_time_seconds = %s,
                                           average_processing_time_seconds = %s,
                                           max_wait_time_seconds = %s,
                                           max_processing_time_seconds = %s,
                                           last_updated = %s
                                       WHERE id = %s
                                       """

                        params = (
                            stat['pending_count'] or 0,
                            stat['in_progress_count'] or 0,
                            stat['completed_count'] or 0,
                            stat['failed_count'] or 0,
                            stat['avg_wait_time'],
                            stat['avg_processing_time'],
                            stat['max_wait_time'],
                            stat['max_processing_time'],
                            current_time,
                            existing[0]['id']
                        )

                        Logger.debug(f"🔍 Ejecutando query: {update_query}")
                        Logger.debug(f"🔍 Parámetros: {params}")
                        self.db_manager.execute_query(update_query, params)
                    else:
                        # Insertar nuevo registro
                        insert_query = """
                                       INSERT INTO queue_metrics
                                       (queue_name, pending_count, in_progress_count, completed_count,
                                        failed_count, average_wait_time_seconds, average_processing_time_seconds,
                                        max_wait_time_seconds, max_processing_time_seconds, last_updated, record_date)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       """

                        params = (
                            queue_name,
                            stat['pending_count'] or 0,
                            stat['in_progress_count'] or 0,
                            stat['completed_count'] or 0,
                            stat['failed_count'] or 0,
                            stat['avg_wait_time'],
                            stat['avg_processing_time'],
                            stat['max_wait_time'],
                            stat['max_processing_time'],
                            current_time,
                            date_only
                        )

                        Logger.debug(f"🔍 Ejecutando query: {insert_query}")
                        Logger.debug(f"🔍 Parámetros: {params}")
                        self.db_manager.execute_query(insert_query, params)
                        Logger.debug(f"🔍 Query ejecutada sin resultados")

            except Exception as e:
                Logger.error(f"Error actualizando métricas de cola: {str(e)}")
                Logger.debug(f"Traceback: {traceback.format_exc()}")

    def _notify_slice_manager(self, message, status, result=None, error_message=None):
        """
        Notifica al Slice Manager sobre el resultado de una operación.

        Args:
            message: Mensaje original
            status: Estado final (COMPLETED, FAILED)
            result: Resultado de la operación (opcional)
            error_message: Mensaje de error (opcional)
        """
        try:
            # Obtener instancia del Slice Manager vía Eureka
            slice_manager = get_service_instance('slice-manager')
            if not slice_manager:
                Logger.error("No se pudo obtener instancia del Slice Manager")
                return

            operation_id = message.get('id')
            operation_type = message.get('operationType')
            user_id = message.get('userId')

            Logger.debug(f"🔍 Instancia encontrada: {json.dumps(slice_manager, indent=2)}")
            Logger.info(f"ℹ Notificando resultado de operación {operation_id} al Slice Manager")

            # Preparar payload de notificación
            notification = {
                "operationId": operation_id,
                "operationType": operation_type,
                "userId": user_id,
                "status": status,
                "timestamp": datetime.now().isoformat()
            }

            if result:
                notification["result"] = result

            if error_message:
                notification["error"] = error_message

            # Enviar notificación
            response = requests.post(
                f"http://{slice_manager['ipAddr']}:{slice_manager['port']}/operation-callback",
                json=notification
            )

            if response.status_code == 200:
                Logger.success(f"✓ Notificación enviada exitosamente")
            else:
                Logger.warning(f"Error enviando notificación: {response.status_code}")
                Logger.debug(f"Respuesta: {response.text}")

        except Exception as e:
            Logger.error(f"Error notificando al Slice Manager: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")

    def _notify_slice_manager_progress(self, message, status, progress_message):
        """
        Notifica al Slice Manager sobre el progreso de una operación.

        Args:
            message: Mensaje original
            status: Estado actual (IN_PROGRESS, etc.)
            progress_message: Mensaje de progreso
        """
        try:
            # Obtener instancia del Slice Manager vía Eureka
            slice_manager = get_service_instance('slice-manager')
            if not slice_manager:
                Logger.error("No se pudo obtener instancia del Slice Manager para progreso")
                return

            operation_id = message.get('id')
            operation_type = message.get('operationType')
            user_id = message.get('userId')

            Logger.info(f"ℹ Notificando progreso de operación {operation_id} al Slice Manager")

            # Preparar payload de notificación de progreso
            notification = {
                "operationId": operation_id,
                "operationType": operation_type,
                "userId": user_id,
                "status": status,
                "message": progress_message,
                "timestamp": datetime.now().isoformat()
            }

            # Enviar notificación
            response = requests.post(
                f"http://{slice_manager['ipAddr']}:{slice_manager['port']}/operation-progress",
                json=notification,
                timeout=10  # Timeout corto para no bloquear
            )

            if response.status_code == 200:
                Logger.success(f"✓ Notificación de progreso enviada exitosamente")
            else:
                Logger.warning(f"Error enviando notificación de progreso: {response.status_code}")
                Logger.debug(f"Respuesta: {response.text}")

        except Exception as e:
            Logger.error(f"Error notificando progreso al Slice Manager: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")

# ===================== API ENDPOINTS =====================
"""
Todas las respuestas de la API siguen este formato:

{
    "status": str,      # Estado de la operación: "success", "error", "partial"
    "message": str,     # Mensaje descriptivo del resultado
    "content": dict     # Datos de respuesta (opcional)
}

Códigos HTTP:
- 200: Éxito
- 207: Éxito parcial
- 400: Error en request
- 401: No autorizado
- 403: Acceso denegado  
- 404: No encontrado
- 500: Error interno
- 503: Servicio no disponible

Ejemplos:
---------

1. Éxito:
{
    "status": "success",
    "message": "Operación completada exitosamente",
    "content": {
        "slice_id": 123,
        "vms": [...]
    }
}

2. Error:
{
    "status": "error", 
    "message": "Error procesando solicitud",
    "details": "Descripción detallada del error"
}

3. Éxito Parcial:
{
    "status": "partial",
    "message": "Algunas operaciones fallaron",
    "content": {
        "successful": [...],
        "failed": [...]
    }
}

Notas:
- El campo 'content' es opcional y su estructura depende del endpoint
- Los errores siempre incluyen un mensaje descriptivo
- Se usa 'details' para información adicional de errores
"""


@app.route('/deploy-slice', methods=['POST'])
def deploy_slice_endpoint():
    """
    Endpoint para desplegar un nuevo slice.

    Request body:
    {
        "slice_info": {
            "id": int,
            "name": str,
            "description": str,
            ...
        },
        "topology_info": {
            "vms": [...],
            "links": [...],
            "interfaces": [...]
        }
    }

    Returns:
        200: Slice desplegado exitosamente
        400: Error en request
        500: Error interno
    """
    try:
        Logger.major_section("API: DEPLOY SLICE")
        
        # Validar request
        config = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(config, indent=2))
        
        if not config:
            Logger.error("No se recibieron datos JSON")
            return jsonify({
                "status": "error",
                "message": "No se recibieron datos JSON",
                "details": "El request debe incluir la configuración del slice"
            }), 400

        # Validar estructura
        required_keys = ['slice_info', 'topology_info']
        missing = [k for k in required_keys if k not in config]
        if missing:
            Logger.error(f"Faltan campos requeridos: {missing}")
            return jsonify({
                "status": "error",
                "message": "Configuración incompleta",
                "details": f"Campos requeridos faltantes: {', '.join(missing)}"
            }), 400

        # Desplegar slice
        Logger.info("Iniciando despliegue...")
        slice_manager = SliceManager()
        deployed_config = slice_manager.deploy_slice(config)
        
        # Log y retornar resultado
        Logger.success("Slice desplegado exitosamente")
        Logger.debug(f"Configuración final desplegada: {json.dumps(deployed_config, indent=2)}")

        return jsonify({
            "status": "success",
            "message": "Slice desplegado exitosamente",
            "content": deployed_config
        }), 200
        
    except Exception as e:
        Logger.error(f"Error desplegando slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error durante el despliegue",
            "details": str(e)
        }), 500

@app.route('/stop-slice/<slice_id>', methods=['POST'])
def stop_slice_endpoint(slice_id: str):
    """
    Detiene todas las VMs de un slice.

    Request body:
    {
        "vms": [{
            "id": int,
            "name": str,
            "physical_server": {...}
        }],
        "workers": {
            "<worker_id>": {
                "name": str,
                "ip": str,
                ...
            }
        }
    }

    Args:
        slice_id: ID del slice a detener
        
    Returns:
        200: Slice detenido exitosamente
        400: ID inválido o request incompleto
        500: Error interno
    """
    try:
        Logger.major_section(f"API: STOP SLICE ID-{slice_id}")
        
        # Validar request
        request_data = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(request_data, indent=2))
        
        if not request_data:
            Logger.error("No se recibieron datos de configuración")
            return jsonify({
                "status": "error", 
                "message": "No se recibieron datos de configuración",
                "details": "El request debe incluir la configuración de VMs y workers"
            }), 400

        # Validar ID
        try:
            slice_id = int(slice_id)
        except ValueError:
            Logger.error(f"ID inválido: {slice_id}")
            return jsonify({
                "status": "error",
                "message": "ID de slice inválido",
                "details": "El ID debe ser un número entero"
            }), 400

        # Validar estructura del request
        required_keys = ['vms', 'workers']
        missing = [k for k in required_keys if k not in request_data]
        if missing:
            Logger.error(f"Faltan campos requeridos: {missing}")
            return jsonify({
                "status": "error",
                "message": "Configuración incompleta",
                "details": f"Campos requeridos faltantes: {', '.join(missing)}"
            }), 400

        # Detener slice
        Logger.info(f"Deteniendo slice {slice_id}...")
        slice_manager = SliceManager()
        success, results = slice_manager.stop_slice(slice_id, request_data)

        if success:
            Logger.success(f"Slice {slice_id} detenido exitosamente")
            Logger.debug(f"Resultados: {json.dumps(results, indent=2)}")
            
            return jsonify({
                "status": "success",
                "message": f"Slice {slice_id} detenido exitosamente",
                "content": {
                    "slice_id": slice_id,
                    "worker_results": results
                }
            }), 200
        else:
            error_msg = results.get('error', 'Error desconocido')
            Logger.error(f"Error deteniendo slice: {error_msg}")
            return jsonify({
                "status": "error",
                "message": "Error deteniendo slice",
                "details": error_msg
            }), 500

    except Exception as e:
        Logger.error(f"Error en stop_slice_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error deteniendo slice",
            "details": str(e)
        }), 500

@app.route('/restart-slice/<slice_id>', methods=['POST']) 
def restart_slice_endpoint(slice_id: str):
    """
    Reinicia todas las VMs de un slice específico.

    Request body:
    {
        "vms": [{
            "id": int,
            "name": str,
            "qemu_pid": int,
            "vnc_display": int,
            "status": str,
            "physical_server": {...}
        }],
        "workers": {
            "<worker_id>": {
                "name": str,
                "ip": str,
                ...
            }
        }
    }

    Args:
        slice_id: ID del slice a reiniciar
        
    Returns:
        200: Slice reiniciado exitosamente
        400: ID inválido o request incompleto 
        500: Error interno
    """
    try:
        Logger.major_section(f"API: RESTART SLICE ID-{slice_id}")
        
        # Validar request
        request_data = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(request_data, indent=2))
        
        if not request_data:
            Logger.error("No se recibieron datos de configuración")
            return jsonify({
                "status": "error",
                "message": "No se recibieron datos de configuración",
                "details": "El request debe incluir la configuración de VMs y workers"
            }), 400

        # Validar ID
        try:
            slice_id = int(slice_id)
        except ValueError:
            Logger.error(f"ID inválido: {slice_id}")
            return jsonify({
                "status": "error",
                "message": "ID de slice inválido",
                "details": "El ID debe ser un número entero"
            }), 400

        # Reiniciar slice
        Logger.info(f"Reiniciando slice {slice_id}...")
        slice_manager = SliceManager()
        success, updated_vms = slice_manager.restart_slice(slice_id, request_data)
        
        if success and updated_vms:
            Logger.success(f"Slice {slice_id} reiniciado exitosamente")
            Logger.debug(f"VMs actualizadas: {json.dumps(updated_vms, indent=2)}")
            
            return jsonify({
                "status": "success",
                "message": f"Slice {slice_id} reiniciado exitosamente",
                "content": {
                    "slice_id": slice_id,
                    "status": "running",
                    "vms": updated_vms
                }
            }), 200
        else:
            Logger.error("Error reiniciando VMs del slice")
            return jsonify({
                "status": "error",
                "message": "Error reiniciando VMs del slice",
                "details": "No se pudieron reiniciar una o más VMs"
            }), 500

    except Exception as e:
        Logger.error(f"Error en restart_slice_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error reiniciando slice",
            "details": str(e)
        }), 500

@app.route('/pause-vm/<vm_id>', methods=['POST'])
def pause_vm_endpoint(vm_id):
    """
    Pausa una VM específica deteniendo su proceso QEMU.

    Request body:
    {
        "vm_info": {
            "id": int,
            "name": str,
            "qemu_pid": int,
            "status": str
        },
        "worker_info": {
            "name": str,
            "ip": str,
            "ssh_username": str,
            ...
        }
    }

    Args:
        vm_id: ID de la VM a pausar

    Returns:
        200: VM pausada exitosamente
        400: Request inválido
        500: Error interno
    """
    try:
        Logger.major_section(f"API: PAUSE VM ID-{vm_id}")
        
        # Validar request
        request_data = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(request_data, indent=2))
        
        if not request_data:
            Logger.error("No se recibieron datos de configuración")
            return jsonify({
                "status": "error",
                "message": "No se recibieron datos de configuración",
                "details": "El request debe incluir la información de la VM y worker"
            }), 400

        # Validar estructura
        required_keys = ['vm_info', 'worker_info']
        missing = [k for k in required_keys if k not in request_data]
        if missing:
            Logger.error(f"Faltan campos requeridos: {missing}")
            return jsonify({
                "status": "error",
                "message": "Configuración incompleta",
                "details": f"Campos requeridos faltantes: {', '.join(missing)}"
            }), 400

        # Pausar VM
        Logger.info(f"Pausando VM ID-{vm_id}...")
        slice_manager = SliceManager()
        
        if slice_manager.pause_vm(request_data):
            vm_info = request_data['vm_info']
            Logger.success(f"VM ID-{vm_id} pausada exitosamente")
            
            return jsonify({
                "status": "success",
                "message": f"VM ID-{vm_id} pausada exitosamente",
                "content": {
                    "vm_id": vm_info['id'],
                    "status": "paused"
                }
            }), 200
        else:
            Logger.error("Error pausando VM")
            return jsonify({
                "status": "error",
                "message": "Error pausando VM",
                "details": "No se pudo detener el proceso QEMU"
            }), 500

    except Exception as e:
        Logger.error(f"Error en pause_vm_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error pausando VM",
            "details": str(e)
        }), 500

@app.route('/resume-vm/<vm_id>', methods=['POST'])
def resume_vm_endpoint(vm_id):
    """
    Reanuda una VM que estaba pausada.

    Request body:
    {
        "vm_info": {
            "id": int,
            "name": str,
            "image_path": str,
            "flavor": {...},
            "status": "paused"
        },
        "worker_info": {
            "name": str,
            "ip": str,
            ...
        }
    }

    Returns:
        200: VM reanudada exitosamente
        400: Request inválido
        500: Error interno
    """
    try:
        Logger.major_section(f"API: RESUME VM ID-{vm_id}")
        
        # Validar request
        request_data = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(request_data, indent=2))

        if not request_data:
            Logger.error("No se recibieron datos de configuración")
            return jsonify({
                "status": "error",
                "message": "No se recibieron datos de configuración",
                "details": "El request debe incluir la información de la VM y worker"
            }), 400

        # Validar estructura
        required_keys = ['vm_info', 'worker_info']
        missing = [k for k in required_keys if k not in request_data]
        if missing:
            Logger.error(f"Faltan campos requeridos: {missing}")
            return jsonify({
                "status": "error",
                "message": "Configuración incompleta",
                "details": f"Campos requeridos faltantes: {', '.join(missing)}"
            }), 400

        # Reanudar VM 
        Logger.info(f"Reanudando VM ID-{vm_id}...")
        slice_manager = SliceManager()
        success, qemu_pid, vnc_display = slice_manager.resume_vm(request_data)

        if success:
            Logger.success(f"VM ID-{vm_id} reanudada exitosamente")
            return jsonify({
                "status": "success",
                "message": f"VM ID-{vm_id} reanudada exitosamente",
                "content": {
                    "vm_id": request_data['vm_info']['id'],
                    "qemu_pid": qemu_pid,
                    "vnc_display": vnc_display,
                    "vnc_port": 5900 + vnc_display,
                    "status": "running"
                }
            }), 200
        else:
            Logger.error("Error reanudando VM")
            return jsonify({
                "status": "error",
                "message": "Error reanudando VM",
                "details": "No se pudo iniciar el proceso QEMU"
            }), 500

    except Exception as e:
        Logger.error(f"Error en resume_vm_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error reanudando VM",
            "details": str(e)
        }), 500

@app.route('/restart-vm/<vm_id>', methods=['POST'])
def restart_vm_endpoint(vm_id: str):
    """
    Reinicia una VM específica.

    Request body:
    {
        "vm_info": {
            "id": int,
            "name": str,
            "qemu_pid": int,
            "image_path": str,
            "flavor": {...}
        },
        "worker_info": {
            "name": str, 
            "ip": str,
            ...
        }
    }

    Returns:
        200: VM reiniciada exitosamente
        400: Request inválido
        500: Error interno
    """
    try:
        Logger.major_section(f"API: RESTART VM ID-{vm_id}")
        
        # Validar request
        request_data = request.get_json()
        Logger.debug("Request recibido:")
        Logger.debug(json.dumps(request_data, indent=2))

        if not request_data:
            Logger.error("No se recibieron datos de configuración")
            return jsonify({
                "status": "error",
                "message": "No se recibieron datos de configuración",
                "details": "El request debe incluir la información de la VM y worker"
            }), 400

        # Validar estructura
        required_keys = ['vm_info', 'worker_info']
        missing = [k for k in required_keys if k not in request_data]
        if missing:
            Logger.error(f"Faltan campos requeridos: {missing}")
            return jsonify({
                "status": "error",
                "message": "Configuración incompleta", 
                "details": f"Campos requeridos faltantes: {', '.join(missing)}"
            }), 400

        # Reiniciar VM
        Logger.info(f"Reiniciando VM ID-{vm_id}...")
        slice_manager = SliceManager()
        success, qemu_pid, vnc_display = slice_manager.restart_vm(request_data)
        
        if success:
            vm_info = request_data['vm_info']
            Logger.success(f"VM ID-{vm_id} reiniciada exitosamente")
            return jsonify({
                "status": "success",
                "message": f"VM {vm_info['name']} reiniciada exitosamente",
                "content": {
                    "vm_id": vm_info['id'],
                    "qemu_pid": qemu_pid,
                    "vnc_display": vnc_display,
                    "vnc_port": 5900 + vnc_display,
                    "status": "running"
                }
            }), 200
        else:
            Logger.error("Error reiniciando VM")
            return jsonify({
                "status": "error",
                "message": "Error reiniciando VM",
                "details": "No se pudo reiniciar el proceso QEMU"
            }), 500

    except Exception as e:
        Logger.error(f"Error en restart_vm_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error reiniciando VM",
            "details": str(e)
        }), 500
    
@app.route('/sync-images', methods=['POST'])
def sync_images_endpoint():
    """
    Sincroniza todas las imágenes base con los workers.
    
    Realiza una sincronización completa de todas las imágenes base 
    disponibles con todos los workers del clúster.

    Returns:
        200: Sincronización completada exitosamente
        207: Sincronización parcial (algunos workers fallaron)
        500: Error en sincronización
    """
    try:
        Logger.major_section("API: SYNC IMAGES")
        
        # Iniciar sincronización
        Logger.info("Iniciando sincronización de imágenes...")
        image_manager = ImageManager()
        results = image_manager.sync_all_images_to_workers()
        
        # Log resultados detallados
        Logger.debug("Resultados por worker:")
        Logger.debug(json.dumps(results, indent=2))
        
        # Análisis de resultados
        any_success = any(result['success'] for result in results.values())
        all_success = all(result['success'] for result in results.values())
        
        successful_workers = [w for w, r in results.items() if r['success']]
        failed_workers = [w for w, r in results.items() if not r['success']]
        
        if not any_success:
            Logger.error("Sincronización fallida en todos los workers")
            return jsonify({
                "status": "error",
                "message": "No se pudo sincronizar con ningún worker",
                "details": "Todos los intentos de sincronización fallaron",
                "content": {
                    "results": results,
                    "failed_workers": failed_workers
                }
            }), 500
            
        if all_success:
            Logger.success("Sincronización completada exitosamente en todos los workers")
            message = "Sincronización completada exitosamente"
            status_code = 200
        else:
            Logger.warning("Sincronización parcial - algunos workers fallaron")
            message = "Sincronización parcial - algunos workers fallaron"
            status_code = 207
            
        return jsonify({
            "status": "success" if all_success else "partial",
            "message": message,
            "content": {
                "results": results,
                "successful_workers": successful_workers,
                "failed_workers": failed_workers
            }
        }), status_code
        
    except Exception as e:
        Logger.error(f"Error en sync_images_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error durante sincronización de imágenes",
            "details": str(e)
        }), 500


@app.route('/kafka-status', methods=['GET'])
def kafka_status_endpoint():
    """
    Endpoint para verificar el estado del procesador Kafka.

    Returns:
        200: Estado del procesador Kafka
    """
    try:
        Logger.section("API: KAFKA STATUS")

        # Verificar estado de consumidores y workers
        worker_status = {}

        for priority, workers in kafka_processor.worker_threads.items():
            worker_status[priority] = {
                "total": len(workers),
                "active": sum(1 for w in workers if w.is_alive())
            }

        consumer_status = {}
        for priority, consumer in kafka_processor.consumers.items():
            consumer_status[priority] = {
                "active": consumer.is_alive()
            }

        # Verificar mensajes en colas
        queue_status = {}
        for priority, queues in kafka_processor.message_queues.items():
            queue_status[priority] = {
                f"partition_{i}": q.qsize()
                for i, q in enumerate(queues)
            }

        # Obtener estadísticas de operaciones
        status_query = """
                       SELECT status, COUNT(*) as count
                       FROM operation_requests
                       GROUP BY status \
                       """
        status_counts = kafka_processor.db_manager.execute_query(status_query)

        type_query = """
                     SELECT operation_type, COUNT(*) as count
                     FROM operation_requests
                     GROUP BY operation_type \
                     """
        type_counts = kafka_processor.db_manager.execute_query(type_query)

        # Obtener métricas de las colas
        metrics_query = """
                        SELECT queue_name, \
                               pending_count, \
                               in_progress_count,
                               completed_count, \
                               failed_count,
                               average_wait_time_seconds, \
                               average_processing_time_seconds
                        FROM queue_metrics
                        WHERE DATE (record_date) = CURDATE() \
                        """
        queue_metrics = kafka_processor.db_manager.execute_query(metrics_query)

        # Preparar respuesta
        status_data = {
            "consumers": consumer_status,
            "workers": worker_status,
            "queues": queue_status,
            "operations": {
                "by_status": {item['status']: item['count'] for item in status_counts},
                "by_type": {item['operation_type']: item['count'] for item in type_counts}
            },
            "queue_metrics": {
                item['queue_name']: {
                    "pending": item['pending_count'],
                    "in_progress": item['in_progress_count'],
                    "completed": item['completed_count'],
                    "failed": item['failed_count'],
                    "avg_wait_time": item['average_wait_time_seconds'],
                    "avg_processing_time": item['average_processing_time_seconds']
                } for item in queue_metrics
            },
            "timestamp": datetime.now().isoformat()
        }

        return jsonify({
            "status": "success",
            "message": "Estado del procesador Kafka",
            "content": status_data
        }), 200

    except Exception as e:
        Logger.error(f"Error en kafka_status_endpoint: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error obteniendo estado de Kafka",
            "details": str(e)
        }), 500

@app.errorhandler(Exception)
def handle_error(error):
    """
    Manejador global de errores para la API.
    
    Procesa cualquier error no manejado y retorna una respuesta JSON consistente.
    Los errores HTTP conocidos retornan mensajes específicos.
    
    Args:
        error: Excepción capturada
        
    Returns:
        Response: Respuesta JSON con formato estándar de error
    """
    try:
        Logger.error(f"Error no manejado: {str(error)}")
        
        # Determinar código y mensaje
        code = 500
        if hasattr(error, 'code'):
            code = error.code
            
        # Mensajes específicos por código
        messages = {
            400: "Solicitud inválida",
            401: "No autorizado",
            403: "Acceso denegado",
            404: "Recurso no encontrado",
            405: "Método no permitido",
            500: "Error interno del servidor",
            503: "Servicio no disponible"
        }
        
        message = messages.get(code, str(error))
        
        # Log detallado para errores del servidor
        if code >= 500:
            Logger.debug(f"Traceback: {traceback.format_exc()}")
        
        return jsonify({
            "status": "error",
            "message": message,
            "details": str(error) if code >= 500 else None,
            "code": code
        }), code
        
    except Exception as e:
        # Fallback para errores durante el manejo de errores
        Logger.error(f"Error en error handler: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error interno del servidor",
            "details": "Error durante el manejo del error original"
        }), 500


# ===================== SERVER =====================
if __name__ == '__main__':
    try:
        Logger.major_section("INICIANDO LINUX DRIVER")

        # 1. Inicializar estructura de directorios
        Logger.info("Verificando estructura de directorios...")
        if not init_directories():
            Logger.error("Error crítico: No se pudo crear la estructura de directorios")
            raise Exception("No se pudo crear la estructura de directorios necesaria")
        Logger.success("Estructura de directorios verificada")

        # 2. Iniciar procesador Kafka
        Logger.section("INICIANDO PROCESADOR KAFKA")
        kafka_processor = KafkaProcessor()
        kafka_processor.start()
        Logger.success("Procesador Kafka iniciado")

        # 3. Iniciar servidor Flask
        Logger.section("INICIANDO SERVIDOR WEB")
        Logger.info("Configuración del servidor:")
        Logger.info(f"- Puerto: {port}")
        Logger.info(f"- Debug: {debug}")

        Logger.debug("Iniciando servidor Flask...")
        Logger.success("Linux Driver listo para recibir conexiones")

        # Manejar cierre al detener el servidor
        try:
            app.run(
                host=host,
                port=port,
                debug=debug
            )
        finally:
            # Detener procesador Kafka al finalizar Flask
            Logger.section("DETENIENDO SERVICIOS")
            kafka_processor.stop()
            Logger.info("Servicios detenidos")

    except Exception as e:
        Logger.error(f"Error crítico durante el inicio: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)