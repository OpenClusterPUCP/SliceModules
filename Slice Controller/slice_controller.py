# ==============================================================================
# | ARCHIVO: server_multi.py
# ==============================================================================
# | DESCRIPCIÓN:
# | Servidor Flask que implementa el backend para el despligue de slices 
# | de red en un clúster. Realiza el despliegue, configuración y gestión de VMs,
# | redes virtuales y recursos distribuidos a través de múltiples nodos workers.
# ==============================================================================
# | CONTENIDO PRINCIPAL:
# | 1. CONFIGURACIÓN INICIAL
# |    - Importaciones y configuración Flask/CORS/WebSocket
# |    - Estructura de directorios y archivos de datos
# |    - Configuración de nodos (workers y OFS)
# |    - Recursos disponibles (imágenes y flavors)
# |    - Por implementar el uso de BD
# |
# | 2. GESTORES/MÓDULOS PRINCIPALES
# |    - SSHManager: Conexiones SSH a nodos
# |    - WorkerAssigner: Asignación de VMs a workers
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
# |    - /vm-vnc, /vm-token: Acceso VNC
# |    - /slice/[id], /vm/[id]: Información y estado
# |    - /sync-images: Sincronización de imágenes
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


# ===================== CONFIGURACIÓN DE FLASK =====================
app = Flask(__name__, static_url_path='/static', static_folder='static')
sock = Sock(app)
CORS(app)


# ===================== CONSTANTES =====================
# Directorio de trabajo principal
WORKING_DIR = os.path.dirname(os.path.abspath(__file__))

# Estructura de directorios
DATA_DIR = {
    'root': os.path.join(WORKING_DIR, 'data'),
    'slices': os.path.join(WORKING_DIR, 'data', 'slices'),
    'network': os.path.join(WORKING_DIR, 'data', 'network'),
    'dhcp': {
        'config': os.path.join(WORKING_DIR, 'data', 'dhcp', 'config'),
        'log': os.path.join(WORKING_DIR, 'data', 'dhcp', 'log')
    },
    'images': os.path.join(WORKING_DIR, 'images'),
    'test': os.path.join(WORKING_DIR, 'test_data')
}

# Archivos de datos
DATA_FILES = {
    'slices': os.path.join(DATA_DIR['slices'], 'slices.json'),
    'networks': os.path.join(DATA_DIR['network'], 'networks.json'),
    'vnc_ports': os.path.join(DATA_DIR['root'], 'vnc_ports.json'),
    'vnc_tokens': os.path.join(DATA_DIR['root'], 'vnc_tokens.json')
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
    'svlan_range': range(1, 4000),
    'slice_network_prefix': '10.69',
    'dhcp_ns_name': 'ns-internet',
    'main_bridge': 'br-int',
    'tap_prefix': 'tap'
}

# Recursos disponibles
AVAILABLE_IMAGES = [
    {
        "id": "img-cirros",
        "name": "CirrOS",
        "path": os.path.join(DATA_DIR['images'], "cirros-0.5.1-x86_64-disk.qcow2")
    },
    {
        "id": "img-ubuntu-noble",
        "name": "Ubuntu Noble",
        "path": os.path.join(DATA_DIR['images'], "focal-server-cloudimg-amd64.img")
    },
    {
        "id": "img-alpine",
        "name": "Alpine Linux",
        "path": os.path.join(DATA_DIR['images'], "alpine-virt.qcow2")
    },
    {
        "id": "img-lubuntu",
        "name": "Lubuntu Desktop",
        "path": os.path.join(DATA_DIR['images'], "lubuntu-20.04.5-desktop-amd64.qcow2")
    }
]

AVAILABLE_FLAVORS = [
    {"id": "flavor-nano", "name": "nano", "ram": 128, "vcpus": 1, "disk": 0.5},
    {"id": "flavor-micro", "name": "micro", "ram": 256, "vcpus": 1, "disk": 1},
    {"id": "flavor-small","name": "small","ram": 512,"vcpus": 2,"disk": 2.5},
    {"id": "flavor-medium","name": "medium","ram": 1024,"vcpus": 2,"disk": 3}
    # RAM en MB, vCPUs en #, disco en GB
]

# Configuración VNC
VNC_CONFIG = {
    'port_start': 5901,
    'port_end': 7000
}


# ===================== UTILIDADES =====================
# Funciones auxiliares para manejo de archivos, directorios, logs e IDs

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

def generate_next_id(file_path: str, key: str) -> int:
    """
    Genera un nuevo ID secuencial basado en IDs existentes en un archivo JSON.
    
    Args:
        file_path: Ruta del archivo JSON que contiene los IDs
        key: Clave del array donde buscar los IDs existentes

    Returns:
        int: Siguiente ID disponible (máximo existente + 1)
    """
    try:
        Logger.debug(f"Generando siguiente ID para {key} en {file_path}")
        
        data = load_json_file(file_path)
        existing_ids = [int(item["id"]) for item in data.get(key, [])]
        
        next_id = max(existing_ids or [0]) + 1
        Logger.debug(f"IDs existentes: {existing_ids}")
        Logger.debug(f"Siguiente ID: {next_id}")
        
        return next_id
        
    except Exception as e:
        Logger.warning(f"Error generando ID, usando 1 como valor por defecto: {str(e)}")
        return 1

def init_data_files():
    """
    Inicializa los archivos de datos necesarios con estructuras básicas.
    
    Crea los siguientes archivos si no existen:
    - slices.json: Lista de slices desplegados
    - networks.json: Configuración de redes
    - vnc_ports.json: Asignación de puertos VNC
    """
    Logger.section("INICIALIZANDO ARCHIVOS DE DATOS")
    
    default_structures = {
        DATA_FILES['slices']: {'slices': []},
        DATA_FILES['networks']: {'networks': []},
        DATA_FILES['vnc_ports']: {'ports': {}}
    }

    for file_name, (file_path, default_content) in zip(
        ["Slices", "Networks", "VNC Ports"],
        default_structures.items()
    ):
        Logger.info(f"Verificando archivo {file_name}...")
        if not os.path.exists(file_path):
            Logger.debug(f"Creando {file_name} con estructura por defecto")
            save_json_file(file_path, default_content)
        else:
            Logger.debug(f"Archivo {file_name} ya existe")

    Logger.success("Inicialización de archivos completada")

def get_slice_id_for_vm(vm_id: int) -> int:
    """
    Obtiene el ID del slice al que pertenece una VM específica.
    
    Args:
        vm_id: ID de la VM a buscar

    Returns:
        int: ID del slice al que pertenece la VM

    Raises:
        Exception: Si la VM no se encuentra en ningún slice
    """
    try:
        Logger.debug(f"Buscando slice para VM {vm_id}")
        slices_data = load_json_file(DATA_FILES['slices'])
        
        for slice_data in slices_data.get('slices', []):
            slice_id = slice_data['slice_info']['id']
            Logger.debug(f"Revisando slice {slice_id}")
            
            for vm in slice_data['topology_info']['vms']:
                if vm['id'] == vm_id:
                    Logger.debug(f"VM {vm_id} encontrada en slice {slice_id}")
                    return slice_id
        
        Logger.error(f"VM {vm_id} no encontrada en ningún slice")
        raise Exception(f"VM {vm_id} no encontrada en ningún slice")
        
    except Exception as e:
        Logger.error(f"Error buscando slice para VM {vm_id}: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        raise

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
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
# Clases y funciones que permiten controlar el ciclo de vida de las VMs y slices

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

class WorkerAssigner:
    """
    Implementa diferentes estrategias de asignación de VMs a workers.
    
    Esta clase proporciona métodos estáticos para asignar VMs a workers del clúster
    usando diferentes algoritmos de distribución, como:
    - Round-robin: Distribución circular secuencial
    - (Futuros algoritmos: carga, recursos disponibles, etc.)
    """
    
    @staticmethod
    def round_robin(vms: list, workers: list) -> dict:
        """
        Asigna VMs a workers usando el algoritmo round-robin (distribución circular).
        
        Esta estrategia asigna las VMs de manera secuencial a los workers disponibles,
        volviendo al primer worker cuando se llega al último. Garantiza una
        distribución uniforme cuando las VMs tienen requerimientos similares.
        
        Args:
            vms (list): Lista de diccionarios con información de las VMs a asignar.
                Cada VM debe tener al menos un campo 'id'.
            workers (list): Lista de nombres/identificadores de workers disponibles.
            
        Returns:
            dict: Diccionario que mapea IDs de VM a nombres de workers asignados.
                Formato: {vm_id: worker_name, ...}
                
        Example:
            >>> assigner = WorkerAssigner()
            >>> vms = [{'id': 1}, {'id': 2}, {'id': 3}]
            >>> workers = ['worker1', 'worker2']
            >>> assigner.round_robin(vms, workers)
            {1: 'worker1', 2: 'worker2', 3: 'worker1'}
        """
        Logger.subsection("ASIGNANDO VMS A WORKERS (Round-Robin)")
        Logger.info(f"VMs a asignar: {len(vms)}")
        Logger.info(f"Workers disponibles: {len(workers)}")
        Logger.debug(f"IDs de VMs: {[vm['id'] for vm in vms]}")
        Logger.debug(f"Workers: {workers}")

        assignments = {}
        for i, vm in enumerate(vms):
            worker = workers[i % len(workers)]
            assignments[vm['id']] = worker
            Logger.debug(f"VM {vm['id']} asignada a {worker}")

        Logger.success(f"Asignación completada: {json.dumps(assignments, indent=2)}")
        return assignments

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
        self.worker_assigner = WorkerAssigner() 
        self.image_manager = ImageManager()
        Logger.debug("SliceManager inicializado con sus dependencias")

    def _get_next_svlan(self) -> int:
        """
        Obtiene el siguiente SVLAN ID disponible para un nuevo slice.
        
        Busca secuencialmente el primer ID disponible, considerando:
        - SVLANs ya en uso (activos o detenidos)
        - Rango válido de SVLANs configurado
        
        Returns:
            int: Siguiente SVLAN ID disponible

        Raises:
            Exception: Si no hay más SVLANs disponibles en el rango permitido
        """
        Logger.debug("Buscando siguiente SVLAN disponible")
        
        # Cargar datos necesarios
        networks = load_json_file(DATA_FILES['networks'])
        slices_data = load_json_file(DATA_FILES['slices'])
        
        # Obtener SVLANs en uso
        used_svlans = {
            s['network_config']['svlan_id'] 
            for s in slices_data.get('slices', [])
        }
        Logger.debug(f"SVLANs en uso: {sorted(list(used_svlans))}")
        
        # Buscar siguiente disponible
        svlan = 1
        while svlan in used_svlans:
            svlan += 1
            
        if svlan > max(NETWORK_CONFIG['svlan_range']):
            Logger.error(f"No hay SVLANs disponibles (máximo: {max(NETWORK_CONFIG['svlan_range'])})")
            raise Exception("No hay SVLANs disponibles")
        
        Logger.success(f"SVLAN {svlan} disponible para usar")
        return svlan

    def get_vm_info(self, vm_id: int) -> dict:
        """
        Obtiene información detallada de una VM específica.
        
        Recopila toda la información disponible de una VM, incluyendo:
        - Datos básicos (ID, nombre, estado)
        - Información del slice al que pertenece
        - Recursos asignados (flavor, imagen)
        - Ubicación física (worker)
        - Interfaces de red y sus conexiones
        - Información VNC
        
        Args:
            vm_id (int): ID de la VM a consultar

        Returns:
            dict: Diccionario con toda la información de la VM

        Raises:
            Exception: Si la VM no existe o hay error obteniendo su información
        """
        try:
            Logger.subsection(f"OBTENIENDO INFORMACIÓN DE VM {vm_id}")
            
            # Cargar datos necesarios
            slices_data = load_json_file(DATA_FILES['slices'])
            vnc_data = load_json_file(DATA_FILES['vnc_ports'])
            
            Logger.debug("Buscando VM en slices...")
            vm_info = None
            slice_info = None
            
            # Buscar VM en todos los slices
            for slice_data in slices_data.get('slices', []):
                for vm in slice_data['topology_info']['vms']:
                    if vm['id'] == vm_id:
                        Logger.info(f"VM encontrada en slice {slice_data['slice_info']['id']}")
                        vm_info = vm
                        slice_info = slice_data['slice_info']
                        
                        # Obtener interfaces
                        Logger.debug("Recopilando interfaces...")
                        vm_interfaces = [
                            iface for iface in slice_data['topology_info']['interfaces']
                            if iface['vm_id'] == vm_id
                        ]
                        Logger.debug(f"Encontradas {len(vm_interfaces)} interfaces")
                        
                        # Obtener links asociados
                        for iface in vm_interfaces:
                            if iface['link_id']:
                                link = next(
                                    (l for l in slice_data['topology_info']['links']
                                    if l['id'] == iface['link_id']),
                                    None
                                )
                                iface['link'] = link
                        
                        vm_info['interfaces'] = vm_interfaces
                        break
                if vm_info:
                    break
                    
            if not vm_info:
                Logger.error(f"VM {vm_id} no encontrada en ningún slice")
                raise Exception(f"VM {vm_id} no encontrada")
                
            # Obtener información VNC
            Logger.debug("Obteniendo información VNC...")
            vnc_allocation = vnc_data.get('ports', {}).get(str(vm_id))
            if vnc_allocation:
                vm_info['vnc'] = {
                    'port': vnc_allocation['port'],
                    'display': vnc_allocation['display'],
                    'worker_ip': NODES[vnc_allocation['worker']]['data_ip']
                }
                Logger.debug(f"VNC: Display {vnc_allocation['display']}, "
                           f"Puerto {vnc_allocation['port']}")
            
            # Obtener recursos
            Logger.debug("Obteniendo información de recursos...")
            flavor = self.resource_manager.get_flavor_config(vm_info['flavor_id'])
            image = next(
                (img for img in AVAILABLE_IMAGES if img['id'] == vm_info['image_id']),
                None
            )
            
            # Construir respuesta
            response = {
                'id': vm_info['id'],
                'name': vm_info['name'],
                'status': vm_info['status'],
                'slice_id': slice_info['id'],
                'slice_name': slice_info['name'],
                'resources': {
                    'flavor': flavor,
                    'image': image
                },
                'location': vm_info['physical_server'],
                'interfaces': vm_info['interfaces'],
                'vnc': vm_info.get('vnc'),
                'qemu_pid': vm_info.get('qemu_pid')
            }
            
            Logger.success(f"Información de VM {vm_id} recopilada exitosamente")
            return response
            
        except Exception as e:
            Logger.error(f"Error obteniendo información de VM {vm_id}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Error obteniendo información de VM: {str(e)}")
        
    def _assign_cvlans(self, links: list) -> None:
        """
        Asigna CVLANs a los enlaces de manera secuencial.
        
        Asigna IDs de VLAN cliente (CVLAN) a cada enlace de la topología,
        comenzando desde 10 e incrementando de 10 en 10 para evitar conflictos.
        Los CVLANs se usan para aislar el tráfico entre redes virtuales dentro
        del mismo slice.

        Args:
            links (list): Lista de enlaces a los que asignar CVLANs
        """
        Logger.subsection("ASIGNANDO CVLANs A ENLACES")
        Logger.info(f"Procesando {len(links)} enlaces")
        
        used_cvlans = set()
        cvlan_base = 10  # Comenzamos desde 10
        
        for link in links:
            # Encontrar siguiente CVLAN disponible
            while cvlan_base in used_cvlans:
                cvlan_base += 10  # Incrementamos de 10 en 10
            
            link['cvlan_id'] = cvlan_base
            used_cvlans.add(cvlan_base)
            cvlan_base += 10
            Logger.debug(f"Enlace {link.get('name', link['id'])}: CVLAN {cvlan_base-10}")
        
        Logger.success("Asignación de CVLANs completada")

    def process_slice_request(self, slice_request: dict) -> dict:
        """
        Procesa una solicitud de creación de slice y prepara su configuración.
        
        Esta función realiza todo el procesamiento inicial necesario para desplegar
        un nuevo slice, incluyendo:
        - Asignación de IDs únicos (slice, VMs, enlaces, interfaces)
        - Validación de recursos y topología
        - Configuración de red y DHCP
        - Asignación de workers a VMs
        - Generación de direcciones MAC
        - Asignación de puertos VNC
        
        Args:
            slice_request (dict): Solicitud de creación con la configuración inicial
            
        Returns:
            dict: Configuración procesada y enriquecida lista para despliegue
            
        Raises:
            Exception: Si hay errores en la configuración o recursos solicitados
        """
        try:
            Logger.major_section("PROCESANDO SOLICITUD DE SLICE")
            
            # 1. Cargar datos existentes y calcular nuevos IDs
            Logger.section("FASE 1: CÁLCULO DE IDs")
            Logger.info("Cargando datos existentes...")
            slices_data = load_json_file(DATA_FILES['slices'])
            
            # Recolectar IDs existentes
            existing_ids = {
                'slices': [],
                'vms': [],
                'links': [],
                'interfaces': []
            }

            for s in slices_data.get('slices', []):
                existing_ids['slices'].append(s['slice_info']['id'])
                
                for vm in s['topology_info']['vms']:
                    existing_ids['vms'].append(vm['id'])
                
                for link in s['topology_info']['links']:
                    existing_ids['links'].append(link['id'])
                
                for interface in s['topology_info']['interfaces']:
                    existing_ids['interfaces'].append(interface['id'])

            # Calcular siguientes IDs
            next_ids = {
                'slice': max(existing_ids['slices'] or [0]) + 1,
                'vm': max(existing_ids['vms'] or [0]) + 1,
                'link': max(existing_ids['links'] or [0]) + 1,
                'interface': max(existing_ids['interfaces'] or [0]) + 1
            }
            
            Logger.debug(f"IDs calculados: {json.dumps(next_ids, indent=2)}")

            # 2. Asignar nuevos IDs
            Logger.section("FASE 2: ASIGNACIÓN DE IDs")
            slice_request['slice_info']['id'] = int(next_ids['slice'])
            
            # Mapear IDs de VMs
            Logger.info("Mapeando IDs de VMs...")
            vm_id_mapping = {}
            for vm in slice_request['topology_info']['vms']:
                old_id = str(vm['id'])
                vm['id'] = next_ids['vm']
                vm_id_mapping[old_id] = next_ids['vm']
                next_ids['vm'] += 1
                Logger.debug(f"VM {old_id} → {vm['id']}")
            
            # Mapear IDs de enlaces
            Logger.info("Mapeando IDs de enlaces...")
            link_id_mapping = {}
            for link in slice_request['topology_info']['links']:
                old_id = str(link['id'])
                link['id'] = next_ids['link']
                link_id_mapping[old_id] = next_ids['link']
                next_ids['link'] += 1
                Logger.debug(f"Link {old_id} → {link['id']}")
            
            # Actualizar referencias en interfaces
            Logger.info("Actualizando referencias de interfaces...")
            for interface in slice_request['topology_info']['interfaces']:
                vm_id = str(interface['vm_id'])
                link_id = str(interface['link_id']) if interface['link_id'] is not None else None
                
                if vm_id in vm_id_mapping:
                    interface['vm_id'] = vm_id_mapping[vm_id]
                if link_id and link_id in link_id_mapping:
                    interface['link_id'] = link_id_mapping[link_id]
                
                interface['id'] = next_ids['interface']
                next_ids['interface'] += 1
                Logger.debug(f"Interface {interface['id']}: VM={interface['vm_id']}, Link={interface['link_id']}")

            # 3. Generar direcciones MAC
            Logger.section("FASE 3: GENERACIÓN DE MACs")
            for interface in slice_request['topology_info']['interfaces']:
                slice_hex = f"{next_ids['slice']:02x}"
                if_hex = f"{interface['id']:02x}"
                vm_hex = f"{interface['vm_id']:02x}"
                interface['mac_address'] = f"52:54:00:{slice_hex}:{if_hex}:{vm_hex}"
                Logger.debug(f"Interface {interface['id']}: MAC={interface['mac_address']}")

            # 4. Validaciones
            Logger.section("FASE 4: VALIDACIONES")
            
            # Verificar VMs en topología
            if not slice_request.get('topology_info', {}).get('vms'):
                Logger.error("No se encontraron VMs en la topología")
                raise ValueError("La topología debe contener al menos una VM")
            
            # Verificar IDs únicos
            vm_ids = [vm['id'] for vm in slice_request['topology_info']['vms']]
            if len(vm_ids) != len(set(vm_ids)):
                Logger.error("Se encontraron IDs de VM duplicados")
                raise ValueError("IDs de VMs deben ser únicos")
            
            # Validar recursos
            Logger.info("Validando recursos solicitados...")
            for vm in slice_request['topology_info']['vms']:
                self.resource_manager.validate_resources(vm)

            # 5. Configuración de red
            Logger.section("FASE 5: CONFIGURACIÓN DE RED")
            
            # Verificar SVLAN
            if next_ids['slice'] not in NETWORK_CONFIG['svlan_range']:
                Logger.error(f"SVLAN {next_ids['slice']} fuera de rango")
                raise Exception(
                    f"ID de slice {next_ids['slice']} fuera del rango permitido para SVLANs "
                    f"({min(NETWORK_CONFIG['svlan_range'])}-{max(NETWORK_CONFIG['svlan_range'])})"
                )

            # Generar configuración de red
            svlan = next_ids['slice']
            network_prefix = f"{NETWORK_CONFIG['slice_network_prefix']}.{svlan}"
            network_config = {
                'slice_id': next_ids['slice'],
                'svlan_id': svlan,
                'network': f"{network_prefix}.0/24",
                'dhcp_range': [
                    f"{network_prefix}.3",
                    f"{network_prefix}.254"
                ],
                'slice_bridge_name': f"br-s{str(next_ids['slice'])}",
                'patch_ports': {
                    'slice_side': f"p-s{svlan}-int",
                    'int_side': f"p-br-s{svlan}"
                },
                'dhcp_interface': f"veth-int.{svlan}",
                'gateway_interface': f"gw-{svlan}"     
            }
            Logger.debug(f"Configuración de red generada: {json.dumps(network_config, indent=2)}")

            # 6. Asignación de workers
            Logger.section("FASE 6: ASIGNACIÓN DE WORKERS")
            workers = [k for k in NODES.keys() if k != 'ofs']
            Logger.info(f"Workers disponibles: {workers}")
            
            worker_assignments = self.worker_assigner.round_robin(
                slice_request['topology_info']['vms'],
                workers
            )
            Logger.debug(f"Asignaciones: {json.dumps(worker_assignments, indent=2)}")

            # 7. Actualizar información de VMs
            Logger.section("FASE 7: ACTUALIZACIÓN DE VMs")
            for vm in slice_request['topology_info']['vms']:
                worker = worker_assignments[vm['id']]
                Logger.info(f"Procesando VM {vm['id']} en {worker}")
                
                # Asignar worker y display VNC
                vnc_info = self.vnc_manager.allocate_port(
                    slice_request['slice_info']['id'],
                    vm['id'],
                    worker
                )
                
                vm.update({
                    'status': 'preparing',
                    'physical_server': {
                        'name': worker,
                        'id': worker.replace('worker', '')
                    },
                    'vnc_display': vnc_info['display']
                })
                Logger.debug(f"VM {vm['id']}: Display VNC={vnc_info['display']}")

                # Ordenar interfaces (externas primero)
                vm_interfaces = [
                    i for i in slice_request['topology_info']['interfaces']
                    if i['vm_id'] == vm['id']
                ]
                vm_interfaces.sort(key=lambda x: not x['external_access'])
                
                # Actualizar lista de interfaces
                slice_request['topology_info']['interfaces'] = [
                    i for i in slice_request['topology_info']['interfaces']
                    if i['vm_id'] != vm['id']
                ] + vm_interfaces
                
                Logger.debug(f"Interfaces ordenadas para VM {vm['id']}")

            # 8. Asignar CVLANs
            Logger.section("FASE 8: ASIGNACIÓN DE CVLANs")
            self._assign_cvlans(slice_request['topology_info']['links'])

            # 9. Preparar respuesta
            Logger.section("COMPLETADO")
            response = {
                'slice_info': slice_request['slice_info'],
                'network_config': network_config,
                'topology_info': slice_request['topology_info']
            }
            
            Logger.success("Procesamiento de slice completado exitosamente")
            return response

        except Exception as e:
            Logger.error(f"Error procesando slice: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Error procesando solicitud de slice: {str(e)}")

    def _parallel_image_transfer(self, worker_images: dict) -> None:
        """
        Transfiere imágenes a workers en paralelo usando múltiples hilos SFTP.
        
        Coordina la transferencia simultánea de imágenes a múltiples workers usando
        hilos independientes para cada uno. Maneja la creación de directorios,
        verificación de imágenes existentes y permisos.
        
        Args:
            worker_images (dict): Mapeo de workers a conjuntos de rutas de imágenes
                Formato: {worker_name: set(image_paths)}
                
        Raises:
            Exception: Si hay errores en la transferencia a cualquier worker
        """
        Logger.section("TRANSFERENCIA PARALELA DE IMÁGENES")
        transfer_threads = []
        transfer_errors = {}
        
        def transfer_to_worker(worker: str, images: set):
            try:
                with SSHManager(worker) as ssh:
                    Logger.subsection(f"Iniciando transferencia a {worker}")
                    sftp = ssh.client.open_sftp()
                    
                    # Crear y preparar directorio
                    Logger.info("Configurando directorio de imágenes...")
                    ssh.execute(f"sudo mkdir -p {DATA_DIR['images']}")
                    ssh.execute(f"sudo chmod 777 {DATA_DIR['images']}")
                    
                    # Transferir imágenes
                    for image_path in images:
                        base_name = os.path.basename(image_path)
                        remote_path = f"{DATA_DIR['images']}/{base_name}"
                        try:
                            sftp.stat(remote_path)
                            Logger.debug(f"Imagen {base_name} ya existe en {worker}")
                        except FileNotFoundError:
                            Logger.info(f"Transfiriendo {base_name} a {worker}")
                            sftp.put(image_path, remote_path)
                                    
                    # Limpiar
                    sftp.close()
                    ssh.execute(f"sudo chmod 755 {DATA_DIR['images']}")
                    ssh.execute(f"sudo chown -R ubuntu:ubuntu {DATA_DIR['images']}")
                    Logger.success(f"Transferencia completada a {worker}")
                        
            except Exception as e:
                error_msg = f"Error en transferencia a {worker}: {str(e)}"
                Logger.error(error_msg)
                transfer_errors[worker] = str(e)
                raise  # Propagar error
                
        # Iniciar threads de transferencia
        Logger.info(f"Iniciando {len(worker_images)} threads de transferencia")
        for worker, images in worker_images.items():
            thread = threading.Thread(
                target=transfer_to_worker,
                args=(worker, images)
            )
            threads.append(thread)
            thread.start()
        
        # Esperar threads
        for thread in threads:
            thread.join()
            
        if transfer_errors:
            raise Exception(f"Errores en transferencia de imágenes: {transfer_errors}")

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
                    Logger.debug(f"\nProcesando VM: {vm['id']}")
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
                                Logger.debug(f"VM {vm['id']} actualizada: "
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

    def _worker_setup(self, worker_info: dict, vms: list, slice_config: dict, 
                    results: dict, errors: dict):
        """
        Configura un worker específico para el despliegue de sus VMs asignadas.
        
        Realiza la configuración completa de un worker incluyendo:
        1. Configuración de bridges OVS
        2. Configuración de interfaces TAP
        3. Preparación de imágenes
        4. Inicio de VMs
        
        Args:
            worker_info (dict): Información del worker a configurar
            vms (list): Lista de VMs a desplegar en este worker
            slice_config (dict): Configuración completa del slice
            results (dict): Diccionario para almacenar resultados
            errors (dict): Diccionario para almacenar errores
        """
        try:
            Logger.subsection(f"CONFIGURANDO WORKER {worker_info['name']}")
            Logger.debug(f"Worker info: {json.dumps(worker_info, indent=2)}")
            Logger.info(f"VMs a configurar: {len(vms)}")

            # Preparar información SSH
            ssh_info = {
                'name': worker_info['name'],
                'ip': worker_info['ip'],
                'ssh_username': worker_info['ssh_username'],
                'ssh_password': worker_info.get('ssh_password'),
                'ssh_key_path': worker_info.get('ssh_key_path')
            }

            with SSHManager(ssh_info) as ssh:
                # 1. Configuración de bridges
                Logger.info(f"Configurando bridges en {worker_info['name']}...")
                bridge_commands = self._generate_bridge_commands(slice_config['network_config'])
                Logger.debug(f"Comandos bridge: {' && '.join(bridge_commands)}")
                ssh.execute(" && ".join(bridge_commands))
                
                # 2. Configuración de interfaces TAP - MODIFICADO
                Logger.info(f"Configurando interfaces TAP en {worker_info['name']}...")
                tap_commands = self._generate_tap_commands(vms, slice_config)
                
                if tap_commands:
                    Logger.debug(f"Ejecutando comandos TAP: {' && '.join(tap_commands)}")
                    ssh.execute(" && ".join(tap_commands))

                # 3. Preparar imágenes
                Logger.info(f"Preparando imágenes en {worker_info['name']}...")
                image_commands = self._generate_image_commands(vms, slice_config)
                if image_commands:
                    Logger.debug(f"Comandos imagen: {' && '.join(image_commands)}")
                    ssh.execute(" && ".join(image_commands))

                # 4. Iniciar VMs
                vm_results = []
                for vm in vms:
                    Logger.info(f"Iniciando VM {vm['id']} en {worker_info['name']}...")
                    self._start_single_vm(ssh, vm, slice_config)
                    
                    # Verificar estado
                    stdout, _ = ssh.execute(
                        f"pgrep -fa 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}'"
                    )
                    if stdout.strip():
                        qemu_pid = int(stdout.split()[0])
                        Logger.success(f"VM {vm['id']} iniciada con PID {qemu_pid}")
                        vm_results.append({
                            'id': vm['id'],
                            'qemu_pid': qemu_pid,
                            'status': 'running'
                        })

                results[worker_info['name']] = {
                    'success': True,
                    'vms': vm_results
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
            Logger.debug(f"Procesando interfaces de VM {vm['id']}")
            vm_interfaces = [i for i in slice_config['topology_info']['interfaces']
                            if i['vm_id'] == vm['id']]
            
            for interface in vm_interfaces:
                tap_name = interface.get('tap_name')
                if not tap_name:
                    Logger.warning(f"Interface sin tap_name para VM {vm['id']}")
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
            Logger.subsection(f"INICIANDO VM {vm['id']}")
            
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
            
            ssh.execute(f"sudo cp {base_path} {vm_image_path}")
            ssh.execute(f"sudo qemu-img resize {vm_image_path} {flavor_info['disk']}G")
            ssh.execute(f"sudo chmod 644 {vm_image_path}")
            
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
                tap_name = interface['tap_name']  # Usar nombre TAP proporcionado por handle_multi
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
            Logger.success(f"VM {vm['id']} iniciada con PID {vm['qemu_pid']}")
            
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
            
            Logger.debug(f"VM {vm['id']}: Imagen {image_id}, Flavor {flavor_id}")
            
            # Generar nombres de archivo
            base_image = os.path.basename(image_info['path'])
            vm_image = f"vm-{str(vm['id'])}-slice-{str(slice_config['slice_info']['id'])}.qcow2"
            vm_image_path = os.path.join('/home/ubuntu/SliceManager/images', vm_image)
            base_image_path = os.path.join('/home/ubuntu/SliceManager/base', base_image)
            
            Logger.debug(f"Base: {base_image_path}")
            Logger.debug(f"VM: {vm_image_path}")

            commands.extend([
                f"test -f {base_image_path} || exit 1",
                f"sudo cp {base_image_path} {vm_image_path}",
                f"sudo qemu-img resize {vm_image_path} {flavor_info['disk']}G",
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

    def _setup_slice_bridges(self, svlan: int, vms: list, network_config: dict):
        """
        Crea y configura bridges específicos para el slice en los workers.
        
        Configura para cada worker usado:
        1. Bridge dedicado para el slice
        2. Patch ports entre el bridge del slice y br-int
        3. QinQ tagging en patch ports
        
        Args:
            svlan (int): ID del SVLAN para el slice
            vms (list): Lista de VMs del slice
            network_config (dict): Configuración de red del slice
        """
        Logger.section(f"CONFIGURANDO BRIDGES DE SLICE (SVLAN {svlan})")
        
        workers_used = set(vm['physical_server']['name'] for vm in vms)
        Logger.info(f"Workers a configurar: {list(workers_used)}")
        
        for worker in workers_used:
            Logger.subsection(f"Configurando worker {worker}")
            with SSHManager(worker) as ssh:
                slice_bridge = network_config['slice_bridge_name']
                patch_slice = network_config['patch_ports']['slice_side']
                patch_int = network_config['patch_ports']['int_side']
                
                Logger.debug(f"Bridge: {slice_bridge}")
                Logger.debug(f"Patch ports: {patch_slice} <-> {patch_int}")

                # Limpiar configuración anterior
                Logger.info("Limpiando configuración anterior...")
                ssh.execute(f"""
                    sudo ovs-vsctl --if-exists del-port br-int {patch_int}
                    sudo ovs-vsctl --if-exists del-port {slice_bridge} {patch_slice}
                    sudo ovs-vsctl --if-exists del-br {slice_bridge}
                """)
                
                # Crear y configurar nuevo bridge
                Logger.info("Creando nueva configuración...")
                cmd = f"""
                sudo ovs-vsctl add-br {slice_bridge}
                sudo ovs-vsctl add-port {slice_bridge} {patch_slice} -- \
                    set interface {patch_slice} type=patch options:peer={patch_int}
                sudo ovs-vsctl add-port br-int {patch_int} -- \
                    set interface {patch_int} type=patch options:peer={patch_slice} -- \
                    set port {patch_int} vlan_mode=dot1q-tunnel tag={svlan}
                """
                ssh.execute(cmd)
                Logger.success(f"Bridge {slice_bridge} configurado")

    def _setup_vm_interfaces(self, slice_config: dict):
        """
        Configura las interfaces de red para las VMs del slice.
        
        Asigna nombres a las interfaces TAP:
        - Interfaces externas: tapx-VM{vm_id}-S{slice_id}
        - Interfaces internas: tap-VM{vm_id}-S{slice_id}-{if_num}
        
        Args:
            slice_config (dict): Configuración completa del slice
        """
        Logger.section("CONFIGURANDO INTERFACES DE RED")
        
        network_config = slice_config['network_config']
        slice_bridge = network_config['slice_bridge_name']
        
        Logger.info("Asignando nombres TAP a interfaces...")
        for vm in slice_config['topology_info']['vms']:
            Logger.debug(f"Procesando VM {vm['id']}")
            
            vm_interfaces = [
                i for i in slice_config['topology_info']['interfaces']
                if i['vm_id'] == vm['id']
            ]
            vm_interfaces.sort(key=lambda x: not x['external_access'])
            
            internal_if_count = 0
            for interface in vm_interfaces:
                if interface['external_access']:
                    tap_name = f"tapx-VM{vm['id']}-S{slice_config['slice_info']['id']}"
                    Logger.debug(f"Interface externa: {tap_name}")
                else:
                    internal_if_count += 1
                    tap_name = f"tap-VM{vm['id']}-S{slice_config['slice_info']['id']}-{internal_if_count}"
                    Logger.debug(f"Interface interna {internal_if_count}: {tap_name}")
                    
                interface['tap_name'] = tap_name
                
        Logger.success("Interfaces de red configuradas")

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
                - svlan_id: ID del SVLAN
                - network: Red del slice (CIDR)
                - dhcp_interface: Nombre interfaz DHCP
                - gateway_interface: Nombre interfaz gateway
        """
        Logger.section("CONFIGURANDO ACCESO A INTERNET")
        
        svlan = network_config['svlan_id']
        network = network_config['network']
        dhcp_if = network_config['dhcp_interface']
        gateway_if = network_config['gateway_interface']
        
        Logger.info(f"SVLAN: {svlan}")
        Logger.info(f"Red: {network}")
        Logger.info(f"Interface DHCP: {dhcp_if}")
        Logger.info(f"Interface Gateway: {gateway_if}")

        # 1. Configurar namespace y veth pair
        Logger.subsection("CONFIGURANDO NAMESPACE Y VETH PAIR")
        os.system(f"ip netns add {NETWORK_CONFIG['dhcp_ns_name']} 2>/dev/null || true")
        os.system("ip link add veth-int type veth peer name veth-ext 2>/dev/null || true")
        os.system(f"ip link set veth-int netns {NETWORK_CONFIG['dhcp_ns_name']}")
        
        # 2. Configurar interfaces
        Logger.subsection("CONFIGURANDO INTERFACES")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link set veth-int up")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link set lo up")
        os.system("ip link set veth-ext up")
        
        # 3. Conectar a OVS y configurar gateway
        Logger.subsection("CONFIGURANDO OVS Y GATEWAY")
        os.system(f"ovs-vsctl --may-exist add-port br-int {gateway_if}")
        os.system(f"ovs-vsctl set interface {gateway_if} type=internal")
        os.system(f"ovs-vsctl set port {gateway_if} tag={svlan}")
        os.system(f"ip link set {gateway_if} up")
        os.system(f"ip addr add {network.replace('0/24', '1/24')} dev {gateway_if}")
        
        # 4. Configurar VLAN subinterface
        Logger.subsection("CONFIGURANDO VLAN SUBINTERFACE")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link add link veth-int name {dhcp_if} type vlan id {svlan}")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link set {dhcp_if} up")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip addr add {network.replace('0/24', '2/24')} dev {dhcp_if}")
        
        # 5. Configurar NAT
        Logger.subsection("CONFIGURANDO NAT Y FORWARDING")
        nat_commands = [
            f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} iptables -t nat -A POSTROUTING -s {network} -j MASQUERADE",
            f"sudo iptables -A FORWARD -s {network} -j ACCEPT",
            f"sudo iptables -A FORWARD -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT",
            f"sudo iptables -t nat -A POSTROUTING -s {network} -o ens3 -j MASQUERADE",
            "sudo sysctl -w net.ipv4.ip_forward=1"
        ]
        
        for cmd in nat_commands:
            Logger.debug(f"Ejecutando: {cmd}")
            os.system(cmd)

        # 6. Verificar estado
        Logger.subsection("VERIFICANDO ESTADO FINAL")
        Logger.debug("Estado de OVS:")
        os.system("ovs-vsctl show")
        Logger.debug(f"Estado de {gateway_if}:")
        os.system(f"ip link show {gateway_if}")
        Logger.debug("Estado de interfaces en namespace:")
        os.system(f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link show")

        # 7. Configurar DHCP
        Logger.subsection("CONFIGURANDO DHCP")
        dhcp_config = self._generate_dhcp_config(network_config)
        Logger.debug("Configuración DHCP generada:")
        Logger.debug(dhcp_config)
        self._apply_dhcp_config(dhcp_config, svlan)

        Logger.success("Configuración de red completada exitosamente")
    
    def _generate_dhcp_config(self, network_config: dict) -> str:
        """
        Genera la configuración del servidor DHCP para el slice.
        
        Args:
            network_config (dict): Configuración de red del slice

        Returns:
            str: Contenido del archivo de configuración DHCP
        """
        Logger.debug("Generando configuración DHCP")
        
        network = network_config['network']
        svlan = network_config['svlan_id']
        range1 = network_config['dhcp_range'][0]
        range2 = network_config['dhcp_range'][1]
        dhcp_ip = network.replace('0/24', '2')
        dhcp_if = network_config['dhcp_interface']
        
        # Definir rutas de archivos
        log_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.log')
        lease_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.leases')
        pid_file = os.path.join(DATA_DIR['dhcp']['log'], f'dnsmasq-{svlan}.pid')
        
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

    def _apply_dhcp_config(self, config: str, svlan: int):
        """
        Aplica la configuración DHCP generada.
        
        1. Guarda la configuración en archivo
        2. Inicia servidor DHCP (dnsmasq)
        3. Verifica estado del servicio
        
        Args:
            config (str): Configuración DHCP a aplicar
            svlan (int): ID del SVLAN para nombrar archivos
            
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
            
            # Ejecutar dnsmasq
            Logger.info("Iniciando servidor DHCP...")
            result = os.system(
                f"sudo ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} "
                f"dnsmasq -C {config_file}"
            )
            
            # Verificar resultado
            if result != 0:
                # Verificar error específico
                verify_cmd = (f"sudo ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} "
                            f"dnsmasq -C {config_file} 2>&1")
                output = subprocess.getoutput(verify_cmd)
                
                if "failed to create listening socket" in output and "Address already in use" in output:
                    Logger.error("Puerto DHCP ya en uso")
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
                Logger.debug(f"VM {vm['id']}: {image_path} -> {worker}")
                
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
                Logger.subsection(f"VM {vm['id']} en {vm['physical_server']['name']}")
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
                    ssh.execute(f"sudo cp {DATA_DIR['images']}/{base_image} {vm_image_path}")
                    ssh.execute(f"sudo qemu-img resize --shrink {vm_image_path} {flavor['disk']}G")
                    
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
                        Logger.success(f"VM {vm['id']} iniciada con PID {vm['qemu_pid']}")
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

            # Agrupar VMs por worker
            vms_by_worker = {}
            for vm in request_data['vms']:
                worker_id = str(vm['physical_server']['id'])
                worker_info = request_data['workers'][worker_id]
                
                vms_by_worker.setdefault(worker_info['name'], {
                    'info': worker_info,
                    'vms': []
                })['vms'].append(vm)

            # Almacenamiento de resultados
            worker_results = {}
            worker_errors = {}

            def stop_worker_vms(worker_name: str, worker_data: dict):
                """Función interna para detener VMs en un worker específico"""
                try:
                    worker_info = worker_data['info']
                    vms = worker_data['vms']
                    
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
                                Logger.info(f"Deteniendo VM {vm['id']}")
                                
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
                                        Logger.warning(f"VM {vm['id']} sigue corriendo, usando kill -9")
                                        ssh.execute(f"sudo kill -9 {qemu_pid}")
                                        time.sleep(1)
                                    else:
                                        Logger.success(f"VM {vm['id']} detenida exitosamente")
                                else:
                                    Logger.debug(f"VM {vm['id']} ya no está corriendo")

                            except Exception as e:
                                if "No such process" not in str(e) and "status=1" not in str(e):
                                    raise
                                Logger.debug(f"VM {vm['id']} ya no está corriendo")

                        # 2. Limpiar recursos
                        cleanup_commands = [
                            f"sudo ovs-vsctl --if-exists del-port br-int p-br-s{slice_id}",
                            f"sudo ovs-vsctl --if-exists del-port br-s{slice_id} p-s{slice_id}-int",
                            f"sudo ovs-vsctl --if-exists del-br br-s{slice_id}",
                            f"sudo ip link show | grep 'tap.*-S{slice_id}' | cut -d':' -f2 | xargs -I{{}} sudo ip link del {{}} 2>/dev/null || true",
                            f"sudo rm -f /home/ubuntu/SliceManager/images/vm-*-slice-{slice_id}*.qcow2"
                        ]

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
            
            vnc_response = requests.post(
                'http://localhost:5001/get-available-vnc-displays',
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
                Logger.debug(f"Worker {worker_id} tiene {len(worker_displays)} displays disponibles: {worker_displays}")
                
                # Obtener VMs para este worker
                worker_vms = [vm for vm in request_data['vms'] if str(vm['physical_server']['id']) == worker_id]
                Logger.debug(f"Worker {worker_id} tiene {len(worker_vms)} VMs para reiniciar")
                
                if len(worker_vms) > len(worker_displays):
                    raise Exception(f"No hay suficientes displays para worker {worker_id}. " 
                                f"Necesita {len(worker_vms)}, disponibles {len(worker_displays)}")
                
                # Pre-asignar displays únicos a cada VM
                for idx, vm in enumerate(worker_vms):
                    display = worker_displays[idx]
                    display_assignments[worker_id][vm['id']] = display
                    Logger.debug(f"Pre-asignando display {display} a VM {vm['id']} en worker {worker_id}")
                
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
                        Logger.info(f"Pausando VM {vm['id']}")
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
                        Logger.info(f"Reanudando VM {vm['id']} con display {assigned_display} en Worker {worker_id}")
                        
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
                            Logger.debug(f"VM {vm['id']} reiniciada exitosamente con display {assigned_display}")

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
                    Logger.info(f"  VM {vm_id} → Display {display}")

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
            Logger.info("Obteniendo display VNC...")
            vnc_response = requests.post(
                'http://localhost:5001/get-available-vnc-displays',
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

                Logger.success(f"VM {vm_info['name']} iniciada con PID {qemu_pid}")
                return True, qemu_pid, vnc_display

        except Exception as e:
            Logger.error(f"Error reanudando VM: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, 0, 0

    def _build_qemu_command(self, vm_name: str, flavor: dict, image_path: str, vnc_display: int, extra_args: list = None) -> str:
        """
        Construye el comando QEMU con los argumentos necesarios.

        Genera el comando completo para iniciar una VM con QEMU/KVM incluyendo:
        - Configuración básica (KVM, nombre)
        - Recursos asignados (RAM, vCPUs)
        - Imagen y formato
        - Display VNC
        - Argumentos adicionales opcionales

        Args:
            vm_name (str): Nombre para la VM en QEMU
            flavor (dict): Configuración de recursos con:
                - ram: RAM en MB
                - vcpus: Número de vCPUs
            image_path (str): Ruta a la imagen de disco
            vnc_display (int): Número de display VNC
            extra_args (list, optional): Argumentos QEMU adicionales

        Returns:
            str: Comando QEMU completo listo para ejecutar
        """
        Logger.debug(f"Construyendo comando QEMU para VM {vm_name}")
        Logger.debug(f"Flavor: {json.dumps(flavor, indent=2)}")
        Logger.debug(f"Imagen: {image_path}")
        Logger.debug(f"Display VNC: {vnc_display}")

        # Comandos base
        cmd = [
            "sudo qemu-system-x86_64",
            "-enable-kvm",
            f"-name guest={vm_name}",
            f"-m {flavor['ram']}",
            f"-smp {flavor['vcpus']}",
            f"-drive file={image_path},format=qcow2",
            f"-vnc 0.0.0.0:{vnc_display}",
            "-daemonize",
            "-D /tmp/qemu.log",  # Agregado para logging
            "-d guest_errors"    # Agregado para debugging
        ]

        # Agregar argumentos extra si existen
        if extra_args:
            Logger.debug(f"Agregando argumentos extra: {extra_args}")
            cmd.extend(extra_args)

        command = " ".join(cmd)
        Logger.debug(f"Comando QEMU generado: {command}")
        return command

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
            Logger.section(f"REINICIANDO VM {request_data['vm_info']['id']}")
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

            Logger.success(f"VM reiniciada exitosamente (PID: {qemu_pid}, Display: {vnc_display})")
            return True, qemu_pid, vnc_display

        except Exception as e:
            Logger.error(f"Error reiniciando VM {request_data['vm_info']['id']}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, 0, 0

    def _cleanup_interfaces_and_bridges(self, worker: str, slice_id: int, svlan: int, interfaces: list):
        """
        Limpia interfaces de red y bridges OVS de un worker en orden correcto.

        Realiza la limpieza en el siguiente orden:
        1. Interfaces TAP (eliminación del switch y del sistema)
        2. Patch ports entre bridges
        3. Bridge del slice
        
        Args:
            worker (str): Nombre del worker a limpiar
            slice_id (int): ID del slice
            svlan (int): ID del SVLAN del slice
            interfaces (list): Lista de interfaces a limpiar
            
        Raises:
            Exception: Si hay errores durante la limpieza
        """
        try:
            Logger.subsection(f"LIMPIANDO INTERFACES EN {worker}")
            
            with SSHManager(worker) as ssh:
                slice_bridge = f"br-s{slice_id}"
                patch_slice = f"p-s{svlan}-int"
                patch_int = f"p-br-s{svlan}"
                
                Logger.debug(f"Bridge: {slice_bridge}")
                Logger.debug(f"Patch ports: {patch_slice} <-> {patch_int}")

                # 1. Limpiar interfaces TAP
                Logger.info("Limpiando interfaces TAP...")
                for interface in interfaces:
                    tap_name = interface.get('tap_name') or (
                        f"tapx-VM{interface['vm_id']}-S{slice_id}" 
                        if interface['external_access'] 
                        else f"tap-VM{interface['vm_id']}-S{slice_id}"
                    )
                    
                    try:
                        # Remover del switch correspondiente
                        if interface['external_access']:
                            ssh.execute(f"sudo ovs-vsctl --if-exists del-port br-int {tap_name}")
                        else:
                            ssh.execute(f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {tap_name}")
                        Logger.debug(f"Interface {tap_name} removida del switch")
                    except Exception as e:
                        Logger.warning(f"Error removiendo {tap_name} del switch: {str(e)}")

                # 2. Remover patch ports
                Logger.info("Limpiando patch ports...")
                ssh.execute(f"sudo ovs-vsctl --if-exists del-port br-int {patch_int}")
                ssh.execute(f"sudo ovs-vsctl --if-exists del-port {slice_bridge} {patch_slice}")
                
                # 3. Eliminar bridge
                Logger.info("Eliminando bridge del slice...")
                ssh.execute(f"sudo ovs-vsctl --if-exists del-br {slice_bridge}")
                
                # 4. Eliminar interfaces TAP del sistema
                Logger.info("Eliminando interfaces TAP del sistema...")
                for interface in interfaces:
                    tap_name = interface.get('tap_name') or (
                        f"tapx-VM{interface['vm_id']}-S{slice_id}" 
                        if interface['external_access'] 
                        else f"tap-VM{interface['vm_id']}-S{slice_id}"
                    )
                    try:
                        ssh.execute(f"sudo ip link set {tap_name} down 2>/dev/null || true")
                        ssh.execute(f"sudo ip tuntap del {tap_name} mode tap 2>/dev/null || true")
                        Logger.debug(f"Interface {tap_name} eliminada del sistema")
                    except Exception as e:
                        Logger.warning(f"Error eliminando {tap_name} del sistema: {str(e)}")

            Logger.success(f"Limpieza de interfaces en {worker} completada")

        except Exception as e:
            Logger.error(f"Error limpiando interfaces en {worker}: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def _cleanup_deployment_state(self, slice_config: dict, current_stage: str = None):
        """
        Limpia los recursos creados durante un despliegue fallido.
        """
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

            # Orden de limpieza
            deployment_order = [
                'init',          # VNC ports
                'network',       # Red y DHCP 
                'images',        # Imágenes base
                'worker_setup'   # Configuración completa
            ]

            # Determinar etapas a limpiar
            if current_stage:
                try:
                    start_index = deployment_order.index(current_stage)
                    stages_to_clean = deployment_order[start_index:]
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
                                Logger.info(f"Matando proceso QEMU de VM {vm['id']}...")
                                ssh.execute(f"sudo pkill -9 -f 'guest=VM{vm['id']}-S{slice_config['slice_info']['id']}' 2>/dev/null || true")
                            except Exception as e:
                                Logger.warning(f"Error matando proceso de VM {vm['id']}: {str(e)}")

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

                if stage == 'init':
                    Logger.info("Limpiando recursos básicos...")
                    for vm in slice_config['topology_info']['vms']:
                        self.vnc_manager.deallocate_port(str(vm['id']))

                elif stage == 'network':
                    Logger.info("Limpiando configuración de red y DHCP...")
                    try:
                        # Matar dnsmasq
                        cmd = f"pgrep -f 'dnsmasq.*{svlan}.conf'"
                        try:
                            pid = subprocess.check_output(cmd, shell=True).decode().strip()
                            if pid:
                                Logger.info(f"Matando proceso dnsmasq (PID: {pid})")
                                os.system(f"sudo kill -9 {pid}")
                        except subprocess.CalledProcessError:
                            Logger.debug("No se encontró proceso dnsmasq activo")

                        # Limpiar red
                        cleanup_commands = [
                            f"sudo pkill -9 -f 'dnsmasq.*{svlan}.conf'",
                            f"ip netns exec {NETWORK_CONFIG['dhcp_ns_name']} ip link del {dhcp_if} 2>/dev/null || true",
                            f"ip link del {gateway_if} 2>/dev/null || true", 
                            f"sudo iptables -D FORWARD -s {network} -j ACCEPT 2>/dev/null || true",
                            f"sudo iptables -D FORWARD -d {network} -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null || true",
                            f"sudo iptables -t nat -D POSTROUTING -s {network} -o ens3 -j MASQUERADE 2>/dev/null || true"
                        ]

                        for cmd in cleanup_commands:
                            os.system(cmd)

                        # Limpiar DHCP
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

@app.route('/slice/<slice_id>', methods=['GET'])
def get_slice(slice_id):
    """
    Obtiene información detallada de un slice específico.

    Args:
        slice_id: ID del slice a consultar
        
    Returns:
        200: Información del slice
        400: ID inválido
        404: Slice no encontrado
        500: Error interno
    """
    try:
        Logger.major_section(f"API: GET SLICE ID-{slice_id}")
        
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

        # Buscar slice
        Logger.info(f"Buscando slice {slice_id}")
        slices_data = load_json_file(DATA_FILES['slices'])
        slice_data = next(
            (s for s in slices_data['slices'] 
             if s['slice_info']['id'] == slice_id),
            None
        )
        
        if not slice_data:
            Logger.error(f"Slice {slice_id} no encontrado")
            return jsonify({
                "status": "error",
                "message": f"No se encontró el slice {slice_id}",
                "details": "El slice solicitado no existe"
            }), 404
            
        # Log y retornar resultado
        Logger.success(f"Slice {slice_id} encontrado")
        Logger.debug(f"Datos: {json.dumps(slice_data, indent=2)}")
            
        return jsonify({
            "status": "success",
            "message": "Información de slice obtenida exitosamente",
            "content": slice_data
        }), 200
        
    except Exception as e:
        Logger.error(f"Error obteniendo slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error obteniendo información del slice",
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

@app.route('/slices', methods=['GET'])
def list_slices():
    """
    Lista todos los slices activos en el sistema.
    
    Returns:
        200: Lista de slices obtenida exitosamente
        500: Error interno obteniendo los datos
    """
    try:
        Logger.major_section("API: LIST SLICES")
        
        Logger.info("Cargando datos de slices...")
        slices_data = load_json_file(DATA_FILES['slices'])
        
        Logger.debug(f"Slices encontrados: {len(slices_data['slices'])}")
        Logger.debug(f"Datos: {json.dumps(slices_data['slices'], indent=2)}")
        
        return jsonify({
            "status": "success",
            "message": "Slices obtenidos exitosamente",
            "content": slices_data['slices']
        }), 200

    except Exception as e:
        Logger.error(f"Error obteniendo slices: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error obteniendo slices",
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
        Logger.info(f"Pausando VM {vm_id}...")
        slice_manager = SliceManager()
        
        if slice_manager.pause_vm(request_data):
            vm_info = request_data['vm_info']
            Logger.success(f"VM {vm_info['name']} pausada exitosamente")
            
            return jsonify({
                "status": "success",
                "message": f"VM {vm_info['name']} pausada exitosamente",
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
        Logger.info(f"Reanudando VM {vm_id}...")
        slice_manager = SliceManager()
        success, qemu_pid, vnc_display = slice_manager.resume_vm(request_data)

        if success:
            Logger.success(f"VM {request_data['vm_info']['name']} reanudada exitosamente")
            return jsonify({
                "status": "success",
                "message": f"VM {request_data['vm_info']['name']} reanudada exitosamente",
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
        Logger.info(f"Reiniciando VM {vm_id}...")
        slice_manager = SliceManager()
        success, qemu_pid, vnc_display = slice_manager.restart_vm(request_data)
        
        if success:
            vm_info = request_data['vm_info']
            Logger.success(f"VM {vm_info['name']} reiniciada exitosamente")
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
    
@app.route('/vm/<vm_id>', methods=['GET'])
def get_vm_endpoint(vm_id: str):
    """
    Obtiene información detallada de una VM específica.

    Args:
        vm_id: ID de la VM a consultar

    Returns:
        200: Información de la VM
        400: ID inválido
        404: VM no encontrada
        500: Error interno
    """
    try:
        Logger.major_section(f"API: GET VM ID-{vm_id}")
        
        # Validar ID
        try:
            vm_id = int(vm_id)
        except ValueError:
            Logger.error(f"ID inválido: {vm_id}")
            return jsonify({
                "status": "error",
                "message": "ID de VM inválido",
                "details": "El ID debe ser un número entero"
            }), 400

        # Obtener información
        Logger.info(f"Buscando VM {vm_id}...")
        slice_manager = SliceManager()
        vm_info = slice_manager.get_vm_info(vm_id)
        
        if not vm_info:
            Logger.error(f"VM {vm_id} no encontrada")
            return jsonify({
                "status": "error",
                "message": f"No se encontró la VM {vm_id}",
                "details": "La VM solicitada no existe"
            }), 404

        Logger.success(f"VM {vm_id} encontrada")
        Logger.debug(f"Información: {json.dumps(vm_info, indent=2)}")
        
        return jsonify({
            "status": "success",
            "message": "Información de VM obtenida exitosamente",
            "content": vm_info
        }), 200
        
    except Exception as e:
        Logger.error(f"Error obteniendo VM: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error obteniendo información de la VM",
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
        Logger.major_section("INICIANDO SLICE CONTROLLER")
        
        # 1. Inicializar estructura de directorios
        Logger.info("Verificando estructura de directorios...")
        if not init_directories():
            Logger.error("Error crítico: No se pudo crear la estructura de directorios")
            raise Exception("No se pudo crear la estructura de directorios necesaria")
        Logger.success("Estructura de directorios verificada")
        
        # 2. Inicializar archivos de datos
        Logger.info("Inicializando archivos de datos...")
        init_data_files()
        Logger.success("Archivos de datos inicializados")

        # 3. Inicializar servicios
        Logger.section("INICIANDO SERVICIOS")
        
        # 4. Iniciar servidor Flask
        Logger.section("INICIANDO SERVIDOR WEB")
        Logger.info("Configuración del servidor:")
        Logger.info("- Host: 0.0.0.0")
        Logger.info("- Puerto: 5000")
        Logger.info("- Debug: Activado")
        
        Logger.success("Slice Manager listo para recibir conexiones")
        Logger.debug("Iniciando servidor Flask...")
        
        app.run(
            host='0.0.0.0',
            port=5000, 
            debug=True
        )
        
    except Exception as e:
        Logger.error(f"Error crítico durante el inicio: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)