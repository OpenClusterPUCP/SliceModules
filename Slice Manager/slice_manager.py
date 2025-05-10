# ==============================================================================
# | ARCHIVO: slice_manager.py  
# ==============================================================================
# | DESCRIPCIÓN:
# | API REST que implementa la interfaz de gestión y administración de slices, actuando como
# | intermediario entre el frontend y el servidor de despliegue/control.
# | Maneja la persistencia en BD, validación de requests, y comunicación con
# | el servidor de despliegue.
# ==============================================================================
# | CONTENIDO PRINCIPAL:
# | 1. CONFIGURACIÓN INICIAL
# |    - Importaciones y configuración Flask/CORS/WebSocket
# |    - Configuración de base de datos MySQL
# |    - Pool de conexiones y transacciones
# |    - Logger personalizado para debugging
# |
# | 2. GESTORES/MÓDULOS PRINCIPALES  
# |    - DatabaseManager: Pool de conexiones MySQL
# |    - VNCTokenManager: Tokens JWT para acceso VNC 
# |    - RequestValidator: Validación de requests
# |    - WorkerAssigner: Asignación de workers y displays
# |    - SliceProcessor: Gestión y procesamiento de slices
# |
# | 3. GESTIÓN DE RECURSOS
# |    - Flavors y tipos de VMs disponibles
# |    - Imágenes de sistemas operativos
# |    - Workers y nodos físicos
# |    - Displays VNC y puertos
# |
# | 4. CONTROL DE SLICES
# |    - Organización del despliegue y creación de nuevos slices
# |    - Orden del control del ciclo de vida (stop/restart)
# |    - Gestión de VMs individuales
# |    - Acceso VNC y tokens de autenticación
# |
# | 5. API ENDPOINTS
# |    - /deploy-slice: Creación de nuevos slices
# |    - /stop-slice, /restart-slice: Control de slices
# |    - /pause-vm, /resume-vm, /restart-vm: Control de VMs
# |    - /vm-vnc, /vm-token: Acceso VNC
# |    - /slice/[id]: Información de slices
# |    - /resources/*: Gestión de recursos
# |
# | 6. UTILIDADES
# |    - Logger personalizado con niveles y colores
# |    - Manejo de fechas y serialización
# |    - Validación de datos y topologías
# |    - Manejo de errores y excepciones
# |    - Transacciones atómicas en BD
# ==============================================================================

# ===================== IMPORTACIONES =====================
from flask import Flask, request, jsonify, render_template
from py_eureka_client import eureka_client
from flask_cors import CORS
from flask_sock import Sock

# Networking y sistema:
import sys
import requests
import traceback
import threading
import datetime
import socket
import select
import time

# Utilidades:
import json
import jwt
import base64
import mysql.connector
from mysql.connector import pooling
from datetime import timedelta
from decimal import Decimal
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from typing import Dict, List, Union, Tuple, Optional 


# ===================== CONFIGURACIÓN DE FLASK =====================
app = Flask(__name__)
sock = Sock(app)
CORS(app)


# ===================== CONFIGURACIÓN DE EUREKA =====================
eureka_server = "http://localhost:8761"

eureka_client.init(
    eureka_server=eureka_server,
    app_name="slice-manager",
    instance_port=5001,
    instance_host="localhost",    
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


# ===================== CONFIGURACIÓN BD =====================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "port": 3306,
    "database": "cloud_v3"
}

POOL_CONFIG = {
    "pool_name": "cloudpool",
    "pool_size": 5,
    **DB_CONFIG
}


# ===================== UTILIDADES =====================
# Funciones auxiliares para manejo de información

def datetime_handler(obj):
    """
    Manejador personalizado para serialización JSON de objetos datetime.
    
    Args:
        obj: Objeto a serializar
        
    Returns:
        str: Representación ISO del datetime
        
    Raises:
        TypeError: Si el objeto no es serializable
    """
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')

def json_handler(obj):
    """
    Manejador personalizado para serialización JSON.
    Maneja tipos especiales como datetime y Decimal.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')

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
# Clases y funciones que permiten gestionar VMs y Slices

class VNCTokenManager:
    """
    Administrador de tokens JWT para acceso VNC.
    
    Esta clase maneja la generación y validación de tokens JWT (JSON Web Tokens)
    que se utilizan para autenticar conexiones VNC a las máquinas virtuales.
    
    Atributos:
        secret_key (str): Clave secreta para firmar los tokens
        token_expiry (timedelta): Tiempo de expiración del token
    """
    
    def __init__(self):
        """
        Inicializa el administrador de tokens VNC.
        
        Note:
            En producción se debe usar una clave secreta más segura y 
            almacenarla de forma segura (ej: variables de entorno)
        """
        self.secret_key = "redes-cloud"  # TODO: Mover a config
        self.token_expiry = timedelta(minutes=10)
        Logger.info("VNCTokenManager inicializado")
        Logger.debug(f"Tiempo de expiración de tokens: {self.token_expiry}")

    def generate_token(self, vm_id: int) -> str:
        """
        Genera un token JWT para acceso VNC a una VM específica.
        
        Args:
            vm_id (int): ID de la máquina virtual
            
        Returns:
            str: Token JWT codificado
            
        Raises:
            Exception: Si hay error generando el token
        """
        try:
            Logger.debug(f"Generando token VNC para VM ID-{vm_id}")
            current_time = datetime.datetime.utcnow()
            
            payload = {
                'vm_id': vm_id,
                'exp': current_time + self.token_expiry,
                'iat': current_time
            }
            Logger.debug(f"Payload del token: {payload}")
            
            # Codificar y decodificar el token a string
            token = jwt.encode(payload, self.secret_key, algorithm='HS256')
            if isinstance(token, bytes):
                token = token.decode('utf-8')
                
            Logger.success(f"Token generado exitosamente para VM ID-{vm_id}")
            return token
            
        except Exception as e:
            Logger.error(f"Error generando token VNC para VM ID-{vm_id}: {str(e)}")
            raise Exception(f"Error generando token VNC: {str(e)}")

    def validate_token(self, token: str, vm_id: int) -> bool:
        """
        Valida un token JWT para acceso VNC.
        
        Verifica que:
        - El token sea válido y no esté expirado
        - El token corresponda a la VM especificada
        
        Args:
            token (str): Token JWT a validar
            vm_id (int): ID de la VM a verificar
            
        Returns:
            bool: True si el token es válido, False en caso contrario
        """
        try:
            Logger.debug(f"Validando token VNC para VM ID-{vm_id}")
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            
            # Verificar que el token corresponde a la VM
            if payload['vm_id'] != int(vm_id):
                Logger.warning(f"Token no corresponde a VM ID-{vm_id}")
                return False
            
            Logger.success(f"Token validado correctamente para VM ID-{vm_id}")    
            return True
            
        except jwt.ExpiredSignatureError:
            Logger.warning(f"Token expirado para VM ID-{vm_id}")
            return False
            
        except jwt.InvalidTokenError as e:
            Logger.error(f"Token inválido para VM ID-{vm_id}: {str(e)}")
            return False
            
        except Exception as e:
            Logger.error(f"Error validando token VNC: {str(e)}")
            return False

class JWTManager:
    """
    Manejador de tokens JWT usando llave pública RSA.
    """
    
    PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy3uQ4UZwnOu7C/Xyp5YA
7j4wtSrf78XHI8z8pKmaa6t7hB/Mj/+p1eVvzbWMWBnAhybg2llTyMp73B2lbduG
EFj5fkYopdVgeBfPiRgePHvrYAou1/lkxZZmApbREnqLWKrreITI2xYVnqzHVr2A
tpiKnDihj0LdggkMoyWz/+P81v6YdHk+sEeWKFXfRrOLgSs6UT158SQY1ES7VfPa
L4oYXt0W5kaMyfpA5+7yyScDUZefU1lfXR9erNTYPNQpytryeSt67cdxuum8Mask
6CqawwYHHqBIlk4weZ19wjwCh/RbXaKo+qIPA9eWupsLeWiw+ysfrnXCArRsff2l
lQIDAQAB
-----END PUBLIC KEY-----"""

    def __init__(self):
        """Inicializa el gestor de JWT"""
        try:
            # Procesar PEM a objeto de clave pública
            self.public_key = serialization.load_pem_public_key(
                self.PUBLIC_KEY_PEM.encode(),
                backend=default_backend()
            )
            Logger.success("Clave pública RSA cargada exitosamente")
        except Exception as e:
            Logger.error(f"Error cargando clave pública: {str(e)}")
            raise

    def get_username_from_token(self, token: str) -> Optional[str]:
        """
        Obtiene el username del token JWT.
        
        Args:
            token (str): Token JWT completo (incluyendo 'Bearer ')
            
        Returns:
            Optional[str]: Username extraído o None si hay error
        """
        try:
            # Remover 'Bearer ' si está presente
            if token.startswith('Bearer '):
                token = token[7:]

            # Decodificar token usando PyJWT
            decoded = jwt.decode(
                token,
                self.PUBLIC_KEY_PEM,  # Usar PEM completo
                algorithms=['RS256'],
                options={"verify_signature": True}
            )
            
            # El username está en el claim 'sub'
            username = decoded.get('sub')
            Logger.debug(f"Username extraído del token: {username}")
            return username
            
        except jwt.ExpiredSignatureError:
            Logger.warning("Token expirado")
            return None
        except jwt.InvalidTokenError as e:
            Logger.error(f"Token inválido: {str(e)}")
            return None
        except Exception as e:
            Logger.error(f"Error procesando token: {str(e)}")
            return None
    
    def validate_token(self, token: str) -> bool:
        """
        Valida un token JWT.
        
        Args:
            token (str): Token JWT a validar
            
        Returns:
            bool: True si el token es válido
        """
        try:
            if token.startswith('Bearer '):
                token = token[7:]
                
            decoded = jwt.decode(
                token,
                self.public_key,
                algorithms=['RS256']
            )
            
            # Verificar expiración
            exp = decoded.get('exp')
            if exp and datetime.fromtimestamp(exp) < datetime.utcnow():
                return False
                
            return True
            
        except:
            return False

    def get_user_id_from_username(self, username: str, db) -> Optional[int]:
        """
        Obtiene el ID del usuario desde la base de datos.
        
        Args:
            username (str): Username a buscar
            db: Instancia de DatabaseManager
            
        Returns:
            Optional[int]: ID del usuario o None si no se encuentra
        """
        try:
            result = db.execute_query(
                "SELECT id FROM user WHERE username = %s",
                (username,)
            )
            if result:
                return result[0]['id']
            return None
        except Exception as e:
            Logger.error(f"Error obteniendo user_id: {str(e)}")
            return None

    def get_role_from_token(self, token: str) -> Optional[str]:
        """
        Obtiene el rol del usuario desde el token JWT.
        
        Args:
            token (str): Token JWT completo (incluyendo 'Bearer ')
            
        Returns:
            Optional[str]: Rol del usuario o None si hay error
        """
        try:
            # Remover 'Bearer ' si está presente
            if token.startswith('Bearer '):
                token = token[7:]

            # Decodificar token usando PyJWT
            decoded = jwt.decode(
                token,
                self.PUBLIC_KEY_PEM,  # Usar PEM completo
                algorithms=['RS256'],
                options={"verify_signature": True}
            )
            
            # El rol está en el claim 'roles'
            roles = decoded.get('roles')
            Logger.debug(f"Roles extraídos del token: {roles}")
            return roles
            
        except jwt.ExpiredSignatureError:
            Logger.warning("Token expirado")
            return None
        except jwt.InvalidTokenError as e:
            Logger.error(f"Token inválido: {str(e)}")
            return None
        except Exception as e:
            Logger.error(f"Error procesando token: {str(e)}")
            return None

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
        try:
            Logger.debug(f"Ejecutando query: {query}")
            if params:
                Logger.debug(f"Parámetros: {params}")
                
            with self.get_connection() as cnx:
                with cnx.cursor(dictionary=True) as cursor:
                    cursor.execute(query, params)
                    if cursor.with_rows:
                        results = cursor.fetchall()
                        Logger.debug(f"Filas obtenidas: {len(results)}")
                        return results
                    Logger.debug("Query ejecutada sin resultados")
                    return []
                    
        except Exception as e:
            Logger.error(f"Error ejecutando query: {str(e)}")
            Logger.debug(f"Query: {query}")
            Logger.debug(f"Parámetros: {params}")
            raise

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

class RequestValidator:
    """
    Validador de requests para creación y gestión de slices.
    
    Esta clase proporciona métodos estáticos para validar:
    - Estructura y campos requeridos en requests de creación de slices
    - Referencias a recursos (flavors, images)
    - Consistencia de topología (VMs, links, interfaces)
    
    Attributes:
        None - Clase con métodos estáticos
    """

    @staticmethod
    def validate_slice_request(request_data: dict) -> Tuple[bool, str]:
        """
        Valida la estructura y contenido de un request para crear slice.
        
        Validaciones:
        1. Estructura básica del request
        2. Campos requeridos en slice_info
        3. Estructura de topology_info
        4. Existencia de flavors e images referenciados
        5. Consistencia de links e interfaces
        
        Args:
            request_data (dict): Datos del request a validar
            
        Returns:
            Tuple[bool, str]: (éxito, mensaje de error si falla)
        """
        try:
            Logger.section("Validando Request de Slice")
            Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

            # 1. Validar estructura básica
            Logger.debug("Validando estructura básica...")
            if not isinstance(request_data, dict):
                Logger.error("Request no es un objeto JSON válido")
                return False, "Request debe ser un objeto JSON"

            required_keys = ['slice_info', 'topology_info']
            for key in required_keys:
                if key not in request_data:
                    Logger.error(f"Falta campo requerido: {key}")
                    return False, f"Falta campo requerido: {key}"

            # 2. Validar slice_info
            Logger.debug("Validando slice_info...")
            slice_info = request_data['slice_info']
            required_slice_fields = ['name', 'description']
            for field in required_slice_fields:
                if field not in slice_info:
                    Logger.error(f"Falta campo en slice_info: {field}")
                    return False, f"Falta campo en slice_info: {field}"

            # 3. Validar topology_info
            Logger.debug("Validando topology_info...")
            topology = request_data['topology_info']
            if not all(key in topology for key in ['vms', 'links', 'interfaces']):
                Logger.error("Estructura de topology_info incompleta")
                return False, "topology_info debe contener: vms, links, interfaces"

            # 4. Validar VMs y recursos
            Logger.debug("Validando VMs y recursos...")
            with mysql.connector.connect(**DB_CONFIG) as conn:
                cursor = conn.cursor(dictionary=True)
                for vm in topology['vms']:
                    # Validar campos requeridos
                    required_vm_fields = ['id', 'name', 'image_id', 'flavor_id']
                    if not all(key in vm for key in required_vm_fields):
                        Logger.error(f"VM '{vm.get('name', 'unknown')}' tiene campos faltantes")
                        return False, f"Cada VM debe tener: {', '.join(required_vm_fields)}"

                    # Validar flavor
                    cursor.execute("SELECT id FROM flavor WHERE id = %s", (vm['flavor_id'],))
                    if not cursor.fetchone():
                        Logger.error(f"Flavor no encontrado: {vm['flavor_id']}")
                        return False, f"Flavor no encontrado: {vm['flavor_id']}"

                    # Validar imagen
                    cursor.execute("SELECT id FROM image WHERE id = %s", (vm['image_id'],))
                    if not cursor.fetchone():
                        Logger.error(f"Imagen no encontrada: {vm['image_id']}")
                        return False, f"Imagen no encontrada: {vm['image_id']}"

            # 5. Validar Links e Interfaces
            Logger.debug("Validando links e interfaces...")
            
            # Validar links
            link_ids = set()
            for link in topology['links']:
                if not all(key in link for key in ['id', 'name']):
                    Logger.error("Link con campos faltantes")
                    return False, "Cada link debe tener: id, name"
                if link['id'] in link_ids:
                    Logger.error(f"ID de link duplicado: {link['id']}")
                    return False, f"ID de link duplicado: {link['id']}"
                link_ids.add(link['id'])

            # Validar interfaces
            vm_ids = {vm['id'] for vm in topology['vms']}
            interface_ids = set()
            for iface in topology['interfaces']:
                # Validar campos requeridos
                if not all(key in iface for key in ['id', 'name', 'vm_id', 'link_id']):
                    Logger.error("Interface con campos faltantes")
                    return False, "Cada interface debe tener: id, name, vm_id, link_id"
                
                # Validar IDs únicos
                if iface['id'] in interface_ids:
                    Logger.error(f"ID de interface duplicado: {iface['id']}")
                    return False, f"ID de interface duplicado: {iface['id']}"
                
                # Validar referencias
                if iface['vm_id'] not in vm_ids:
                    Logger.error(f"Interface referencia VM inexistente: {iface['vm_id']}")
                    return False, f"Interface referencia a VM inexistente: {iface['vm_id']}"
                
                # Validar link_id para interfaces no externas
                if iface['link_id'] not in link_ids and not iface.get('external_access'):
                    Logger.error(f"Interface referencia link inexistente: {iface['link_id']}")
                    return False, f"Interface referencia a Link inexistente: {iface['link_id']}"
                
                interface_ids.add(iface['id'])
                
                # Validar external_access
                if 'external_access' in iface and not isinstance(iface['external_access'], bool):
                    Logger.error("external_access debe ser booleano")
                    return False, "external_access debe ser un valor booleano"

            Logger.success("Validación completada exitosamente")
            return True, ""

        except Exception as e:
            Logger.error(f"Error en validate_slice_request: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, f"Error validando request: {str(e)}"

    def validate_resources(self, flavor_id: str, image_id: str) -> Tuple[bool, str]:
        """
        Valida que los recursos solicitados existan y estén activos.
        
        Args:
            flavor_id (str): ID del flavor a validar
            image_id (str): ID de la imagen a validar
            
        Returns:
            Tuple[bool, str]: (éxito, mensaje de error si falla)
        """
        try:
            Logger.debug(f"Validando recursos - Flavor: {flavor_id}, Image: {image_id}")

            # Validar flavor
            flavor = db.execute_query(
                "SELECT id FROM flavor WHERE id = %s AND state = 'active'",
                (flavor_id,)
            )
            if not flavor:
                Logger.error(f"Flavor {flavor_id} no encontrado o inactivo")
                return False, f"Flavor no encontrado o inactivo: {flavor_id}"

            # Validar imagen
            image = db.execute_query(
                "SELECT id FROM image WHERE id = %s AND state = 'active'",
                (image_id,)
            )
            if not image:
                Logger.error(f"Imagen {image_id} no encontrada o inactiva")
                return False, f"Imagen no encontrada o inactiva: {image_id}"

            Logger.success("Recursos validados exitosamente")
            return True, ""
            
        except Exception as e:
            Logger.error(f"Error validando recursos: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, f"Error validando recursos: {str(e)}"

class WorkerAssigner:
    """
    Asignador de workers y displays VNC para máquinas virtuales.
    
    Esta clase maneja la asignación de:
    - Workers físicos para hospedar VMs
    - Displays VNC disponibles en cada worker
    - Distribución balanceada de VMs entre workers
    
    Attributes:
        db: Instancia de DatabaseManager para consultas a BD
    """
    
    def __init__(self, db):
        """
        Inicializa el asignador de workers.
        
        Args:
            db: Instancia de DatabaseManager
        """
        self.db = db
        Logger.info("WorkerAssigner inicializado")

    def get_active_workers(self) -> List[Dict]:
        """
        Obtiene la lista de workers activos disponibles.
        
        Returns:
            List[Dict]: Lista de workers con sus IDs y nombres
                [{"id": int, "name": str}, ...]
                
        Raises:
            Exception: Si hay error consultando la BD
        """
        try:
            Logger.debug("Consultando workers activos...")
            workers = self.db.execute_query(
                """SELECT id, name 
                   FROM physical_server 
                   WHERE server_type = 'worker' 
                   AND status = 'active'
                   LIMIT 3""" # Temporal para pruebas, ya que solo usamos los workers de un mismo slice!
            )
            Logger.info(f"Workers activos encontrados: {len(workers)}")
            Logger.debug(f"Workers: {[w['name'] for w in workers]}")
            return workers
            
        except Exception as e:
            Logger.error(f"Error obteniendo workers activos: {str(e)}")
            raise

    def get_worker_vnc_displays(self, worker_id: int) -> List[int]:
        """
        Obtiene displays VNC disponibles para un worker específico.
        
        Busca displays en uso y encuentra números disponibles en el rango 1-100.
        
        Args:
            worker_id (int): ID del worker
            
        Returns:
            List[int]: Lista de números de display disponibles
            
        Raises:
            Exception: Si hay error consultando la BD
        """
        try:
            Logger.debug(f"Consultando displays VNC para worker {worker_id}")
            
            # Obtener displays en uso
            query = """
                SELECT vnc_display 
                FROM virtual_machine 
                WHERE physical_server = %s 
                AND status != 'stopped' 
                AND vnc_display IS NOT NULL
                ORDER BY vnc_display
            """
            used_displays = [
                vm['vnc_display'] 
                for vm in self.db.execute_query(query, (worker_id,))
                if vm['vnc_display'] is not None
            ]
            
            Logger.debug(f"Displays en uso: {used_displays}")
            
            # Calcular displays disponibles
            available_displays = []
            current = 1
            
            if not used_displays:
                Logger.debug("No hay displays en uso, retornando rango completo")
                return list(range(1, 101))
                
            # Encontrar huecos en la secuencia
            for display in used_displays:
                while current < display:
                    available_displays.append(current)
                    current += 1
                current = display + 1
                
            # Agregar displays después del último usado
            while len(available_displays) < 100:
                available_displays.append(current)
                current += 1
                
            Logger.debug(f"Displays disponibles: {len(available_displays)}")
            Logger.debug(f"Primer display disponible: {available_displays[0] if available_displays else None}")
            
            return available_displays
            
        except Exception as e:
            Logger.error(f"Error obteniendo displays VNC: {str(e)}")
            raise

    def assign_workers_and_displays(self, vms: List[Dict]) -> Dict[int, Dict]:
        """
        Asigna workers y displays VNC a las VMs usando round-robin.
        Asegura que cada VM en un mismo worker tenga un display VNC único.
        """
        try:
            Logger.section(f"Asignando workers y displays para {len(vms)} VMs")
            
            # Obtener workers activos
            workers = self.get_active_workers()
            if not workers:
                Logger.error("No hay workers activos disponibles")
                raise Exception("No hay workers activos disponibles")
            
            Logger.info(f"Workers disponibles: {len(workers)}")
            
            # Diccionario para rastrear displays usados por worker
            worker_displays_used = {}
            worker_available_displays = {}
            
            # Inicializar displays disponibles por worker
            for worker in workers:
                worker_id = worker['id']
                displays = self.get_worker_vnc_displays(worker_id)
                worker_available_displays[worker_id] = displays
                worker_displays_used[worker_id] = set()
                Logger.debug(f"Worker {worker['name']} (ID-{worker_id}): {len(displays)} displays disponibles")
                Logger.debug(f"Displays disponibles: {displays[:5]}...")

            # Asignar VMs usando round-robin
            assignments = {}
            assigned_workers = []  # Para mantener el orden de asignación

            for i, vm in enumerate(vms):
                # Seleccionar worker por round-robin
                worker = workers[i % len(workers)]
                worker_id = worker['id']
                assigned_workers.append(worker['name'])
                
                Logger.debug(f"\nAsignando VM-{vm['id']}:")
                Logger.debug(f"- Worker seleccionado: {worker['name']} (ID-{worker_id})")
                Logger.debug(f"- Displays ya usados en este worker: {worker_displays_used[worker_id]}")
                
                # Obtener siguiente display disponible para este worker
                available_displays = worker_available_displays[worker_id]
                used_displays = worker_displays_used[worker_id]
                
                # Encontrar el primer display disponible que no esté en uso
                display = None
                for d in available_displays:
                    if d not in used_displays:
                        display = d
                        used_displays.add(d)  # Marcar como usado
                        Logger.debug(f"- Display asignado: {display}")
                        break
                        
                if display is None:
                    Logger.error(f"No hay displays VNC disponibles en worker {worker['name']}")
                    raise Exception(f"No hay displays VNC disponibles en worker {worker['name']}")
                
                assignments[vm['id']] = {
                    'worker': {
                        'id': worker['id'],
                        'name': worker['name']
                    },
                    'vnc_display': display
                }
                
                Logger.info(
                    f"VM ID-{vm['id']} asignada a Worker con nombre {worker['name']} (ID-{worker_id}) con Display N°{display}"
                )

            # Resumen de asignaciones
            Logger.info("\nResumen de asignaciones:")
            Logger.info(f"Workers utilizados en orden: {', '.join(assigned_workers)}")
            for worker_id, used_set in worker_displays_used.items():
                worker_name = next(w['name'] for w in workers if w['id'] == worker_id)
                Logger.info(f"Worker {worker_name} (ID-{worker_id}) - Displays asignados: {sorted(used_set)}")

            Logger.success(f"Asignación completada: {len(assignments)} VMs distribuidas")
            return assignments
                
        except Exception as e:
            Logger.error(f"Error en assign_workers_and_displays: {str(e)}")
            raise
    
class SliceProcessor:
    """
    Procesador de solicitudes y recursos para slices.
    
    Esta clase maneja:
    - Generación de IDs secuenciales para recursos (slices, VMs, links, etc.)
    - Gestión de VLANs (SVLAN/CVLAN)
    - Asignación de displays VNC
    - Obtención de información de recursos (flavors, images, workers)
    
    Attributes:
        db: Instancia global de DatabaseManager
        worker_assigner: Instancia de WorkerAssigner para asignar workers
    """
    
    def __init__(self):
        """Inicializa el procesador de slices"""
        self.db = db  # Usar el DatabaseManager global
        self.worker_assigner = WorkerAssigner(db)
        Logger.info("SliceProcessor inicializado")
        
    def get_next_slice_id(self) -> int:
        """
        Obtiene el siguiente ID disponible para slice.
        
        Returns:
            int: Siguiente ID disponible (>= 100)
        """
        try:
            Logger.debug("Obteniendo siguiente ID de slice")
            result = self.db.execute_query("SELECT MAX(id) as max_id FROM slice")
            next_id = (result[0]['max_id'] or 100) + 1
            Logger.debug(f"Siguiente ID de slice: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente ID de slice: {str(e)}")
            raise
        
    def get_next_svlan(self) -> int:
        """
        Obtiene el siguiente SVLAN ID disponible.
        
        Returns:
            int: Siguiente SVLAN ID (>= 100)
        """
        try:
            Logger.debug("Obteniendo siguiente SVLAN ID")
            result = self.db.execute_query(
                "SELECT MAX(svlan_id) as max_id FROM slice_network"
            )
            next_id = (result[0]['max_id'] or 100) + 1
            Logger.debug(f"Siguiente SVLAN ID: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente SVLAN ID: {str(e)}")
            raise
        
    def get_next_cvlan(self) -> int:
        """
        Obtiene el siguiente CVLAN ID disponible.
        
        Returns:
            int: Siguiente CVLAN ID (>= 1)
        """
        try:
            Logger.debug("Obteniendo siguiente CVLAN ID")
            result = self.db.execute_query(
                "SELECT MAX(cvlan_id) as max_id FROM link"
            )
            next_id = (result[0]['max_id'] or 0) + 1
            Logger.debug(f"Siguiente CVLAN ID: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente CVLAN ID: {str(e)}")
            raise
    
    def get_next_vm_id(self) -> int:
        """
        Obtiene el siguiente ID disponible para VMs.
        
        Returns:
            int: Siguiente VM ID (>= 1)
        """
        try:
            Logger.debug("Obteniendo siguiente ID de VM")
            result = self.db.execute_query(
                "SELECT MAX(id) as max_id FROM virtual_machine"
            )
            next_id = (result[0]['max_id'] or 0) + 1
            Logger.debug(f"Siguiente ID de VM: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente ID de VM: {str(e)}")
            raise

    def get_next_link_id(self) -> int:
        """
        Obtiene el siguiente ID disponible para links.
        
        Returns:
            int: Siguiente link ID (>= 1)
        """
        try:
            Logger.debug("Obteniendo siguiente ID de link")
            result = self.db.execute_query(
                "SELECT MAX(id) as max_id FROM link"
            )
            next_id = (result[0]['max_id'] or 0) + 1
            Logger.debug(f"Siguiente ID de link: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente ID de link: {str(e)}")
            raise

    def get_next_interface_id(self) -> int:
        """
        Obtiene el siguiente ID disponible para interfaces.
        
        Returns:
            int: Siguiente interface ID (>= 1)
        """
        try:
            Logger.debug("Obteniendo siguiente ID de interface")
            result = self.db.execute_query(
                "SELECT MAX(id) as max_id FROM interface"
            )
            next_id = (result[0]['max_id'] or 0) + 1
            Logger.debug(f"Siguiente ID de interface: {next_id}")
            return next_id
        except Exception as e:
            Logger.error(f"Error obteniendo siguiente ID de interface: {str(e)}")
            raise
    
    def get_available_vnc_displays(self) -> List[int]:
        """
        Obtiene números de display VNC disponibles.
        
        Busca displays en uso y encuentra números disponibles en el rango 1-100.
        
        Returns:
            List[int]: Lista de números de display disponibles
        """
        try:
            Logger.debug("Obteniendo displays VNC disponibles")
            
            # Obtener displays en uso
            query = """
                SELECT vnc_display 
                FROM virtual_machine 
                WHERE status != 'stopped'
                AND status != 'paused' 
                ORDER BY vnc_display
            """
            used_displays = [
                vm['vnc_display'] 
                for vm in self.db.execute_query(query)
            ]
            Logger.debug(f"Displays en uso: {used_displays}")
            
            # Calcular displays disponibles
            available_displays = []
            current = 1
            
            if not used_displays:
                Logger.debug("No hay displays en uso, retornando rango completo")
                return list(range(1, 101))
                
            # Encontrar huecos en la secuencia
            for display in used_displays:
                while current < display:
                    available_displays.append(current)
                    current += 1
                current = display + 1
            
            # Agregar displays después del último usado
            while len(available_displays) < 100:
                available_displays.append(current)
                current += 1
                
            Logger.debug(f"Displays disponibles: {len(available_displays)}")
            return available_displays
            
        except Exception as e:
            Logger.error(f"Error obteniendo displays VNC: {str(e)}")
            raise
    
    def get_resource_info(self, flavor_id: str, image_id: str) -> Tuple[Dict, Dict]:
        """
        Obtiene información detallada de flavor e imagen.
        
        Args:
            flavor_id (str): ID del flavor
            image_id (str): ID de la imagen
            
        Returns:
            Tuple[Dict, Dict]: (info_flavor, info_imagen)
        """
        try:
            Logger.debug(f"Obteniendo info de recursos - Flavor: {flavor_id}, Image: {image_id}")
            
            # Obtener flavor
            flavor = self.db.execute_query(
                """SELECT id, name, vcpus, ram, disk 
                   FROM flavor 
                   WHERE id = %s""", 
                (flavor_id,)
            )[0]
            
            # Obtener imagen
            image = self.db.execute_query(
                """SELECT id, name, path
                   FROM image 
                   WHERE id = %s""", 
                (image_id,)
            )[0]
            
            Logger.debug(f"Info obtenida - Flavor: {flavor['name']}, Image: {image['name']}")
            return flavor, image
            
        except Exception as e:
            Logger.error(f"Error obteniendo info de recursos: {str(e)}")
            raise

    def get_worker_info(self, worker_id: int) -> Dict:
        """
        Obtiene información detallada del worker.
        
        Args:
            worker_id (int): ID del worker físico
            
        Returns:
            Dict: Información completa del worker
        """
        try:
            Logger.debug(f"Obteniendo información del worker {worker_id}")
            
            worker = self.db.execute_query(
                """SELECT id, name, ip, data_ip, 
                    ssh_username, ssh_password, ssh_key_path,
                    gateway_access_ip, gateway_access_port, 
                    switch_name
                FROM physical_server 
                WHERE id = %s""",
                (worker_id,)
            )[0]
            
            worker_info = {
                'id': worker['id'],
                'name': worker['name'],
                'ip': worker['ip'],
                'data_ip': worker['data_ip'],
                'ssh_username': worker['ssh_username'],
                'ssh_password': worker['ssh_password'],
                'ssh_key_path': worker['ssh_key_path'],
                'gateway_access_ip': worker['gateway_access_ip'],
                'gateway_access_port': worker['gateway_access_port'],
                'switch_name': worker['switch_name']
            }
            
            Logger.debug(f"Información obtenida para worker {worker['name']}")
            return worker_info
            
        except Exception as e:
            Logger.error(f"Error obteniendo info del worker: {str(e)}")
            raise

    def assign_worker(self, vm_index: int) -> dict:
        """
        Asigna un worker para una VM usando distribución round-robin.
        
        Asigna workers activos de forma balanceada usando el índice de la VM
        para distribuir la carga entre los workers disponibles.

        Args:
            vm_index (int): Índice de la VM para asignación round-robin

        Returns:
            dict: Información del worker asignado
                {
                    "name": str,  # Nombre del worker 
                    "id": int     # ID del worker
                }

        Raises:
            Exception: Si no hay workers activos disponibles
        """
        try:
            Logger.debug(f"Asignando worker para VM índice {vm_index}")
            
            workers = self.db.execute_query(
                """SELECT id, name 
                FROM physical_server 
                WHERE server_type = 'worker' 
                AND status = 'active'"""
            )
            
            if not workers:
                Logger.error("No hay workers activos disponibles")
                raise Exception("No hay workers disponibles")
                
            selected_worker = workers[vm_index % len(workers)]
            Logger.success(f"Worker {selected_worker['name']} asignado para VM índice {vm_index}")
            
            return {
                "name": selected_worker['name'],
                "id": selected_worker['id']
            }
            
        except Exception as e:
            Logger.error(f"Error asignando worker: {str(e)}")
            raise

    def generate_network_config(self, slice_id: int, svlan_id: int) -> dict:
        """
        Genera la configuración de red para un slice.

        Crea la configuración de red incluyendo:
        - Red y rango DHCP basados en SVLAN ID
        - Nombres de bridges y puertos patch
        - Interfaces para DHCP y gateway

        Args:
            slice_id (int): ID de la slice
            svlan_id (int): ID de SVLAN asignado

        Returns:
            dict: Configuración completa de red
                {
                    "slice_id": int,       # ID de la slice
                    "svlan_id": int,       # ID de SVLAN
                    "network": str,        # Ej: "10.69.8.0/24"
                    "dhcp_range": list,    # [start_ip, end_ip]
                    "slice_bridge_name": str,
                    "patch_ports": {
                        "slice_side": str, # Ej: "p-s8-int"
                        "int_side": str    # Ej: "p-br-s8"
                    },
                    "dhcp_interface": str,
                    "gateway_interface": str
                }
        """
        Logger.debug(f"Generando configuración de red - Slice: ID-{slice_id}, SVLAN: {svlan_id}")
    
        # Calcular el último octeto para la red (usar módulo para mantener en rango válido)
        network_octet = svlan_id % 256
        
        config = {
            "slice_id": slice_id,
            "svlan_id": svlan_id,
            "network": f"10.{network_octet}.0.0/24",
            "dhcp_range": [
                f"10.{network_octet}.0.3",
                f"10.{network_octet}.0.254"
            ],
            "slice_bridge_name": f"br-s{slice_id}",
            "patch_ports": {
                "slice_side": f"p-s{slice_id}-int",
                "int_side": f"p-br-s{slice_id}"
            },
            "dhcp_interface": f"veth-int.{slice_id}",
            "gateway_interface": f"gw-{slice_id}"
        }
        
        Logger.debug(f"Configuración generada: {json.dumps(config, indent=2)}")
        return config

    def generate_mac_address(self, slice_id: int, interface_id: int, vm_id: int) -> str:
        """
        Genera una dirección MAC única para una interfaz.
        
        Usa el formato QEMU estándar (52:54:00) seguido de identificadores únicos:
        - 52:54:00 - Prefijo estándar QEMU
        - SS - Hash del slice_id (2 dígitos hex)
        - II - Hash del interface_id (2 dígitos hex)
        - VV - Hash del vm_id (2 dígitos hex)

        Args:
            slice_id (int): ID de la slice (cualquier entero positivo)
            interface_id (int): ID de la interfaz (cualquier entero positivo) 
            vm_id (int): ID de la VM (cualquier entero positivo)

        Returns:
            str: Dirección MAC en formato XX:XX:XX:XX:XX:XX
        """
        Logger.debug(
            f"Generando MAC - Slice: ID-{slice_id}, "
            f"Interface: ID-{interface_id}, VM: ID-{vm_id}"
        )

        # Convertir IDs grandes a valores de 2 dígitos hex usando módulo
        slice_hex = f"{slice_id % 256:02x}"
        if_hex = f"{interface_id % 256:02x}"  
        vm_hex = f"{vm_id % 256:02x}"

        mac = f"52:54:00:{slice_hex}:{vm_hex}:{if_hex}"
        Logger.debug(f"MAC generada: {mac}")
        
        return mac

    def process_request(self, request_data: dict) -> dict:
        """
        Procesa la solicitud y genera la configuración completa de la slice.

        Este método realiza:
        1. Generación de IDs secuenciales para todos los recursos
        2. Preparación de información básica de la slice
        3. Generación de configuración de red
        4. Procesamiento y mapeo de VMs, links e interfaces
        5. Asignación de workers y displays VNC
        6. Recopilación de información de recursos

        Args:
            request_data (dict): Datos del request con:
                - slice_info (dict): Información básica de la slice
                - topology_info (dict): Descripción de VMs, links e interfaces

        Returns:
            dict: Configuración completa con:
                - slice_info: Info actualizada de la slice
                - network_config: Configuración de red
                - topology_info: Topología procesada
                - resources_info: Info de recursos asignados

        Raises:
            Exception: Si hay error en el procesamiento
        """
        try:
            Logger.section("PROCESANDO REQUEST DE SLICE")
            Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

            # Generar IDs secuenciales
            Logger.subsection("Generando IDs secuenciales")
            
            # Obtener siguiente ID de slice (desde 100)
            Logger.debug("Obteniendo siguiente ID de slice")
            result = db.execute_query("SELECT MAX(id) as max_id FROM slice")
            next_slice_id = max(100, (result[0]['max_id'] or 99) + 1)
            Logger.debug(f"Siguiente ID de slice: {next_slice_id}")

            # Usar el mismo ID para el SVLAN
            svlan_id = next_slice_id
            Logger.debug(f"SVLAN ID asignado: {svlan_id}")

            # Resto de IDs (mantener la lógica existente para otros IDs)
            Logger.debug("Obteniendo siguiente ID de VM")
            result = db.execute_query("SELECT MAX(id) as max_id FROM virtual_machine")
            next_vm_id = (result[0]['max_id'] or 0) + 1

            Logger.debug("Obteniendo siguiente ID de link")
            result = db.execute_query("SELECT MAX(id) as max_id FROM link")
            next_link_id = (result[0]['max_id'] or 0) + 1

            Logger.debug("Obteniendo siguiente ID de interface")
            result = db.execute_query("SELECT MAX(id) as max_id FROM interface")
            next_interface_id = (result[0]['max_id'] or 0) + 1

            Logger.info(f"IDs generados:")
            Logger.info(f"                - Slice ID: {next_slice_id}")
            Logger.info(f"                - SVLAN ID: {svlan_id}")
            Logger.info(f"                - VM Start ID: {next_vm_id}")
            Logger.info(f"                - Link Start ID: {next_link_id}")
            Logger.info(f"                - Interface Start ID: {next_interface_id}")

            slice_id = next_slice_id
            vm_start_id = next_vm_id
            link_start_id = next_link_id
            interface_start_id = next_interface_id
            base_cvlan = 10

            # 2. Preparar información de la slice
            Logger.subsection("Preparando información de la slice")
            slice_info = request_data['slice_info']
            slice_info['id'] = slice_id
            slice_info['status'] = 'preparing'

            # 3. Generar configuración de red
            Logger.subsection("Generando configuración de red")
            network_config = self.generate_network_config(slice_id, svlan_id)

            # 4. Procesar topología
            Logger.subsection("Procesando topología")
            topology = request_data['topology_info']
            vm_id_mapping = {}
            link_id_mapping = {}

            # Verificar displays VNC
            available_displays = self.get_available_vnc_displays()
            if len(available_displays) < len(topology['vms']):
                Logger.error("No hay suficientes puertos VNC disponibles")
                raise Exception("No hay suficientes puertos VNC disponibles")

            # Asignar workers y displays
            worker_assignments = self.worker_assigner.assign_workers_and_displays(topology['vms'])
            
            Logger.info("Asignaciones de workers y displays VNC:")
            for vm_id, assignment in worker_assignments.items():
                Logger.debug(
                    f"VM ID-{vm_id}: Worker={assignment['worker']['name']}, "
                    f"Display={assignment['vnc_display']}"
                )

            # 4.1 Procesar VMs
            Logger.debug("Procesando VMs...")
            for i, vm in enumerate(topology['vms']):
                old_vm_id = vm['id']
                new_vm_id = vm_start_id + i
                vm_id_mapping[old_vm_id] = new_vm_id

                assignment = worker_assignments[old_vm_id]
                vm['id'] = new_vm_id
                vm['status'] = 'preparing'
                vm['physical_server'] = assignment['worker']
                vm['vnc_display'] = assignment['vnc_display']
                
                Logger.debug(
                    f"VM con nombre '{vm['name']}': ID {old_vm_id}->{new_vm_id}, "
                    f"Worker={vm['physical_server']['name']}"
                )

            # 4.2 Procesar Links
            Logger.debug("Procesando Links...")
            for i, link in enumerate(topology['links']):
                old_link_id = link['id']
                new_link_id = link_start_id + i
                link_id_mapping[old_link_id] = new_link_id

                link['id'] = new_link_id
                link['cvlan_id'] = base_cvlan + (i * 10)
                
                Logger.debug(f"Link {link['name']}: ID {old_link_id}->{new_link_id}, CVLAN={link['cvlan_id']}")

            # 4.3 Procesar Interfaces
            Logger.debug("Procesando Interfaces...")
            vm_interface_counters = {}
            for i, iface in enumerate(topology['interfaces']):
                new_if_id = interface_start_id + i
                old_vm_id = iface['vm_id']
                old_link_id = iface['link_id']
                new_vm_id = vm_id_mapping[old_vm_id]

                if new_vm_id not in vm_interface_counters:
                    vm_interface_counters[new_vm_id] = 1

                if iface.get('external_access', False):
                    new_link_id = None
                    iface['tap_name'] = f"tapx-VM{new_vm_id}-S{slice_id}"
                    iface['external_access'] = True
                else:
                    new_link_id = link_id_mapping.get(old_link_id)
                    if new_link_id is None:
                        Logger.error(f"Link ID {old_link_id} no encontrado para interfaz {iface['name']}")
                        raise Exception(f"Link ID {old_link_id} no encontrado para la interfaz {iface['name']}")
                    
                    iface['tap_name'] = f"tap-VM{new_vm_id}-S{slice_id}-{vm_interface_counters[new_vm_id]}"
                    iface['external_access'] = False
                    vm_interface_counters[new_vm_id] += 1

                iface['id'] = new_if_id
                iface['vm_id'] = new_vm_id
                iface['link_id'] = new_link_id
                iface['mac_address'] = self.generate_mac_address(slice_id, new_if_id, new_vm_id)
                
                Logger.debug(
                    f"Interface con nombre '{iface['name']}': ID-{new_if_id}, "
                    f"VM ID-{old_vm_id} -> ID-{new_vm_id}, "
                    f"Link ID-{old_link_id} -> ID-{new_link_id}"
                )

            # 5. Recopilar información de recursos
            Logger.subsection("Recopilando información de recursos")
            resources_info = {
                'flavors': {},
                'images': {},
                'workers': {}
            }

            for vm in topology['vms']:
                # Info de flavor e imagen
                if vm['flavor_id'] not in resources_info['flavors']:
                    flavor, image = self.get_resource_info(vm['flavor_id'], vm['image_id'])
                    resources_info['flavors'][vm['flavor_id']] = flavor
                    resources_info['images'][vm['image_id']] = image
                    Logger.debug(f"Agregada info de Flavor {flavor['name']} e Image {image['name']}")

                # Info del worker
                worker_id = vm['physical_server']['id']
                if worker_id not in resources_info['workers']:
                    worker_info = self.get_worker_info(worker_id)
                    resources_info['workers'][worker_id] = worker_info
                    Logger.debug(f"Agregada info de Worker {worker_info['name']}")

            # 6. Construir respuesta final
            Logger.success("Request procesado exitosamente")
            return {
                "slice_info": slice_info,
                "network_config": network_config,
                "topology_info": topology,
                "resources_info": resources_info
            }

        except Exception as e:
            Logger.error(f"Error procesando request: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Error procesando solicitud: {str(e)}")

    def save_deployed_slice(self, deployed_data: dict) -> bool:
        """
        Guarda los datos de la slice después de un despliegue exitoso.

        Guarda en la base de datos:
        1. Información básica de la slice
        2. Configuración de red
        3. Links de la topología
        4. VMs con sus asignaciones
        5. Interfaces de red

        Args:
            deployed_data (dict): Datos de la slice desplegado
                - slice_info (dict): Info básica de la slice
                - network_config (dict): Configuración de red
                - topology_info (dict): VMs, links e interfaces

        Returns:
            bool: True si se guardó exitosamente

        Raises:
            Exception: Si hay error guardando los datos
        """
        try:
            Logger.section(f"Guardando Slice Desplegado ID-{deployed_data['slice_info']['id']}")
            slice_info = deployed_data['slice_info']
            network_config = deployed_data['network_config']
            topology = deployed_data['topology_info']
            
            queries = []
            
            # 1. Query para slice
            Logger.debug("Preparando query de slice")
            queries.append((
                """INSERT INTO slice 
                (id, name, description, status, created_at) 
                VALUES (%s, %s, %s, %s, NOW())""",
                (slice_info['id'], slice_info['name'], 
                slice_info['description'], slice_info['status'])
            ))
            
            # 2. Query para network config
            Logger.debug("Preparando query de network config")
            queries.append((
                """INSERT INTO slice_network 
                (slice_id, svlan_id, network, dhcp_range_start, dhcp_range_end, 
                    slice_bridge_name, patch_port_slice, patch_port_int, 
                    dhcp_interface, gateway_interface)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (network_config['slice_id'], network_config['svlan_id'], 
                network_config['network'],
                network_config['dhcp_range'][0], network_config['dhcp_range'][1],
                network_config['slice_bridge_name'], 
                network_config['patch_ports']['slice_side'],
                network_config['patch_ports']['int_side'],
                network_config['dhcp_interface'],
                network_config['gateway_interface'])
            ))
            
            # 3. Queries para links
            Logger.debug(f"Preparando queries para {len(topology['links'])} links")
            for link in topology['links']:
                queries.append((
                    """INSERT INTO link 
                    (id, name, cvlan_id, slice_id) 
                    VALUES (%s, %s, %s, %s)""",
                    (link['id'], link['name'], link['cvlan_id'], slice_info['id'])
                ))
                Logger.debug(f"Link {link['name']}: ID={link['id']}, CVLAN={link['cvlan_id']}")
            
            # 4. Queries para VMs
            Logger.debug(f"Preparando queries para {len(topology['vms'])} VMs")
            for vm in topology['vms']:
                queries.append((
                    """INSERT INTO virtual_machine 
                    (id, name, image, flavor, slice, physical_server, 
                        status, vnc_port, vnc_display, qemu_pid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (vm['id'], vm['name'], vm['image_id'], vm['flavor_id'], 
                    slice_info['id'], vm['physical_server']['id'], 
                    vm['status'], 5900 + vm['vnc_display'], 
                    vm['vnc_display'], vm.get('qemu_pid'))
                ))
                Logger.debug(
                    f"VM con nombre '{vm['name']}': ID={vm['id']}, "
                    f"Worker={vm['physical_server']['name']}, "
                    f"VNC display={vm['vnc_display']}"
                )
            
            # 5. Queries para interfaces
            Logger.debug(f"Preparando queries para {len(topology['interfaces'])} interfaces")
            for iface in topology['interfaces']:
                queries.append((
                    """INSERT INTO interface 
                    (id, name, mac, ip, vm, link, external_access, tap_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (iface['id'], iface['name'], iface['mac_address'],
                    None,  # IP se agrega después si existe
                    iface['vm_id'], iface['link_id'], 
                    iface.get('external_access', False),
                    iface['tap_name'])
                ))
                Logger.debug(
                    f"Interface con nombre '{iface['name']}': ID={iface['id']}, "
                    f"VM-ID={iface['vm_id']}, TAP={iface['tap_name']}"
                )
            
            # Ejecutar transacción
            Logger.debug(f"Ejecutando {len(queries)} queries en transacción")
            success = self.db.execute_transaction(queries)
            
            if success:
                Logger.success("Slice guardado exitosamente en la base de datos")
            return success
                
        except Exception as e:
            Logger.error(f"Error guardando slice: {str(e)}")
            Logger.debug(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Error guardando slice desplegado: {str(e)}")


# ===================== API ENDPOINTS - SLICE =====================
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

Notas:
- El campo 'content' es opcional y su estructura depende del endpoint
- Los errores siempre incluyen un mensaje descriptivo
- Se usa 'details' para información opcional y adicional de errores
"""


# ===================== SLICE =====================
@app.route('/deploy-slice', methods=['POST'])
def deploy_slice():
    """
    Endpoint para desplegar un nuevo slice.

    Request body:
    {
        "slice_info": {
            "name": str, 
            "description": str
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
        request_data = request.get_json()
        Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

        # 1. Validar estructura básica del request
        if 'slice_info' not in request_data:
            Logger.error("Request sin slice_info")
            return jsonify({
                "status": "error",
                "message": "Request inválido",
                "details": "El campo slice_info es requerido"
            }), 400

        slice_info = request_data['slice_info']
        required_fields = ['name', 'description', 'user_id', 'sketch_id']
        for field in required_fields:
            if field not in slice_info:
                Logger.error(f"Campo requerido faltante: {field}")
                return jsonify({
                    "status": "error",
                    "message": "Faltan campos requeridos",
                    "details": f"El campo '{field}' es requerido en slice_info"
                }), 400

        user_id = slice_info['user_id']
        sketch_id = slice_info['sketch_id']

        # 2. Obtener y validar sketch
        Logger.debug(f"Consultando sketch ID-{sketch_id}")
        sketch_query = """
            SELECT s.*, u.username 
            FROM sketch s
            JOIN user u ON s.user = u.id
            WHERE s.id = %s
        """
        sketch_data = db.execute_query(sketch_query, (sketch_id,))

        if not sketch_data:
            Logger.error(f"Sketch ID-{sketch_id} no encontrado")
            return jsonify({
                "status": "error",
                "message": "Sketch no encontrado",
                "details": f"No existe un sketch con ID {sketch_id}"
            }), 404

        sketch = sketch_data[0]
        structure = json.loads(sketch['structure'])
        
        # 3. Validar acceso a recursos
        Logger.debug("Validando acceso a recursos...")
        topology = structure['topology_info']
        
        # Obtener IDs únicos
        flavor_ids = {str(vm['flavor_id']) for vm in topology.get('vms', [])}
        image_ids = {str(vm['image_id']) for vm in topology.get('vms', [])}

        # Verificar flavors - públicos o del usuario
        Logger.debug(f"Verificando flavors: {flavor_ids}")
        valid_flavors = db.execute_query(
            """SELECT id FROM flavor 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(flavor_ids) if flavor_ids else 'NULL', user_id)
        )
        valid_flavor_ids = {str(f['id']) for f in valid_flavors}
        
        invalid_flavors = flavor_ids - valid_flavor_ids
        if invalid_flavors:
            Logger.error(f"Flavors inválidos o no accesibles: {invalid_flavors}")
            return jsonify({
                "status": "error",
                "message": "Algunos flavors no son válidos o no tienes acceso a ellos",
                "details": f"Los siguientes flavors no existen, no están activos o no tienes permiso: {list(invalid_flavors)}"
            }), 400

        # Verificar images - públicas o del usuario
        Logger.debug(f"Verificando images: {image_ids}")
        valid_images = db.execute_query(
            """SELECT id FROM image 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(image_ids) if image_ids else 'NULL', user_id)
        )
        valid_image_ids = {str(i['id']) for i in valid_images}
        
        invalid_images = image_ids - valid_image_ids
        if invalid_images:
            Logger.error(f"Images inválidas o no accesibles: {invalid_images}")
            return jsonify({
                "status": "error",
                "message": "Algunas imágenes no son válidas o no tienes acceso a ellas",
                "details": f"Las siguientes imágenes no existen, no están activas o no tienes permiso: {list(invalid_images)}"
            }), 400

        # 4. Construir configuración para despliegue
        Logger.debug("Preparando configuración para despliegue")
        deployment_config = {
            "slice_info": {
                "name": slice_info['name'],
                "description": slice_info['description'],
                "status": "deploying"
            },
            "topology_info": structure['topology_info']
        }

        # 5. Procesar solicitud y desplegar
        Logger.subsection("Procesando configuración...")
        processor = SliceProcessor()
        try:
            processed_config = processor.process_request(deployment_config)
        except Exception as e:
            Logger.error(f"Error procesando request: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Error generando la configuración de la slice",
                "details": str(e)
            }), 500

        # 6. Enviar a Slice Controller para despliegue
        Logger.subsection("Enviando configuración a Slice Controller")
        try:
            slice_controller = get_service_instance('slice-controller')
            if not slice_controller:
                raise Exception("Servicio slice-controller no disponible")

            server_response = requests.post(
                f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/deploy-slice",
                json=json.loads(json.dumps(processed_config, default=json_handler)),
                timeout=300
            )
            Logger.debug(f"Respuesta Slice Controller: {server_response.status_code}")
            Logger.debug(f"Contenido: {json.dumps(server_response.json(), indent=2)}")
            
        except requests.exceptions.Timeout:
            Logger.error("Timeout en comunicación con Slice Controller")
            return jsonify({
                "status": "error",
                "message": "El servidor de despliegue no respondió a tiempo",
                "details": "La operación excedió el tiempo máximo de espera (300s)"
            }), 503
        except requests.RequestException as e:
            Logger.error(f"Error de comunicación: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Error comunicándose con el servidor de despliegue",
                "details": str(e)
            }), 500

        if server_response.status_code != 200:
            Logger.error(f"Error en Slice Controller: {server_response.text}")
            return jsonify({
                "status": "error",
                "message": "El servidor de despliegue reportó un error",
                "details": server_response.text
            }), server_response.status_code

        # 7. Procesar respuesta y actualizar configuración
        Logger.subsection("Procesando respuesta del servidor")
        try:
            server_data = server_response.json()
            
            # Validar estructura de respuesta
            if not isinstance(server_data, dict):
                raise ValueError(f"Formato de respuesta inválido: {server_data}")
            if 'content' not in server_data:
                raise ValueError(f"Respuesta sin campo 'content': {server_data}")
            if 'topology_info' not in server_data['content']:
                raise ValueError(f"Respuesta sin topology_info: {server_data['content']}")

            # Actualizar configuración
            deployed_config = processed_config
            deployed_config['slice_info']['status'] = 'running'
            
            # Actualizar estado de VMs
            server_content = server_data['content']
            vm_updates = []
            for vm in deployed_config['topology_info']['vms']:
                server_vm = next(
                    (v for v in server_content['topology_info']['vms'] 
                    if v['id'] == vm['id']), 
                    None
                )
                if server_vm:
                    vm['qemu_pid'] = server_vm.get('qemu_pid')
                    vm['status'] = 'running' if vm['qemu_pid'] else 'error'
                    vm_updates.append(f"VM ID-{vm['id']} con nombre '{vm['name']}': {vm['status']}")
                else:
                    Logger.warning(f"No se encontró info de VM ID-{vm['id']}")

        except Exception as e:
            Logger.error(f"Error procesando respuesta: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Error procesando la respuesta del servidor",
                "details": str(e)
            }), 500

        # 8. Guardar configuración y crear propiedad
        Logger.subsection("Guardando configuración en BD")
        try:
            # Guardar slice
            if not processor.save_deployed_slice(deployed_config):
                raise Exception("Error guardando la slice")

            slice_id = deployed_config['slice_info']['id']

            # Crear entrada en tabla property
            property_query = """
                INSERT INTO property (user, slice) 
                VALUES (%s, %s)
            """
            db.execute_transaction([
                (property_query, (user_id, slice_id))
            ])
            Logger.debug(f"Entrada creada en property: User ID-{user_id}, Slice ID-{slice_id}")

        except Exception as e:
            Logger.error(f"Error guardando configuración: {str(e)}")
            return jsonify({
                "status": "error",
                "message": "Error guardando la configuración de la slice",
                "details": str(e)
            }), 500

        # 9. Retornar respuesta exitosa
        Logger.success("Despliegue completado exitosamente")
        return jsonify({
            "status": "success",
            "message": f"Slice '{deployed_config['slice_info']['name']}' desplegada exitosamente",
            "content": deployed_config,
            "details": {
                "slice_id": slice_id,
                "user_id": user_id,
                "sketch_id": sketch_id,
                "vm_status": vm_updates
            }
        }), 200
    
    except Exception as e:
        Logger.error(f"Error general en deploy_slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno durante el despliegue de la slice",
            "details": str(e)
        }), 500

@app.route('/pause-vm/<vm_id>', methods=['POST'])
def pause_vm_endpoint(vm_id: str):
    """
    Endpoint para pausar una VM específica.

    Args:
        vm_id (str): ID de la VM a pausar

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: VM pausada exitosamente
            400: ID de VM inválido
            404: VM no encontrada o no ejecutándose 
            500: Error interno
            503: Error de comunicación con servidor
    """
    try:
        Logger.major_section(f"API: PAUSE VM ID-{vm_id}")
        vm_id = int(vm_id)

        # 0. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 1. Verificar existencia de VM y acceso del usuario
        Logger.debug(f"Verificando acceso del usuario {user_id} a VM {vm_id}")
        access_query = """
            SELECT vm.*, vm.slice as slice_id,
                   ps.name as worker_name, ps.id as worker_id, 
                   ps.ip as worker_ip,
                   ps.ssh_username, ps.ssh_password, ps.ssh_key_path,
                   p.user as property_user
            FROM virtual_machine vm
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN slice s ON vm.slice = s.id
            JOIN property p ON s.id = p.slice
            WHERE vm.id = %s 
            AND vm.status = 'running'
            AND p.user = %s"""

        vm_info = db.execute_query(access_query, (vm_id, user_id))

        if not vm_info:
            # Verificar si la VM existe
            vm_exists = db.execute_query(
                """SELECT id FROM virtual_machine 
                   WHERE id = %s AND status = 'running'""",
                (vm_id,)
            )
            
            if not vm_exists:
                Logger.warning(f"VM ID-{vm_id} no encontrada o no está en ejecución")
                return jsonify({
                    "status": "error",
                    "message": "La máquina virtual no está disponible",
                    "details": "La VM no existe o no se encuentra en estado running"
                }), 404
            else:
                Logger.warning(f"Usuario {user_id} no tiene acceso a VM {vm_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para pausar esta máquina virtual"
                }), 403

        # Continuar con el código existente...
        vm = vm_info[0]
        Logger.info(f"VM ID-{vm['id']} con nombre '{vm['name']}' encontrada en Worker ID-{vm['worker_id']} con nombre {vm['worker_name']}")

        
        # 2. Preparar datos para Slice Controller
        Logger.debug("Preparando datos para servidor de despliegue")
        pause_data = {
            "vm_info": {
                "id": vm['id'],
                "name": vm['name'],
                "qemu_pid": vm['qemu_pid'],
                "vnc_port": vm['vnc_port'],
                "vnc_display": vm['vnc_display'],
                "status": vm['status']
            },
            "worker_info": {
                "id": vm['physical_server'],
                "name": vm['worker_name'],
                "ip": vm['worker_ip'],
                "ssh_username": vm['ssh_username'],
                "ssh_password": vm['ssh_password'],
                "ssh_key_path": vm['ssh_key_path']
            },
            "slice_id": vm['slice_id']
        }
        
        # 3. Enviar solicitud a Slice Controller
        Logger.info("Enviando solicitud de pausa al servidor de despliegue")
        slice_controller = get_service_instance('slice-controller')
        if not slice_controller:
            raise Exception("Servicio slice-controller no disponible") 

        response = requests.post(
            f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/pause-vm/{vm_id}",
            json=pause_data,
            timeout=300
        )
        Logger.debug(f"Respuesta recibida: {response.status_code}")

        # 4. Procesar respuesta
        if response.status_code == 200:
            Logger.info(f"VM ID-{vm_id} con nombre '{vm['name']}' pausada exitosamente en servidor")
            
            # 5. Actualizar estado en BD
            Logger.debug("Actualizando estado en base de datos")
            update_query = """
                UPDATE virtual_machine 
                SET status = 'paused',
                    qemu_pid = NULL,
                    vnc_port = NULL,
                    vnc_display = NULL
                WHERE id = %s
            """
            db.execute_transaction([(update_query, (vm_id,))])
            Logger.success(f"VM ID-{vm_id} con nombre '{vm['name']}' pausada y BD actualizada")

            return jsonify({
                "status": "success",
                "message": f"La máquina virtual con nombre '{vm['name']}' ha sido pausada exitosamente",
                "content": {
                    "vm_id": vm['id'],
                    "name": vm['name'],
                    "status": "paused"
                }
            }), 200
        else:
            Logger.error(f"Error en servidor de despliegue: {response.text}")
            return jsonify({
                "status": "error",
                "message": "Error al intentar pausar la máquina virtual",
                "details": response.json().get('message', 'Error desconocido en servidor')
            }), response.status_code

    except ValueError:
        Logger.error(f"ID de VM inválido: ID-{vm_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la máquina virtual es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except requests.RequestException as e:
        Logger.error(f"Error de comunicación con servidor: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error de comunicación con el servidor de despliegue",
            "details": f"No se pudo establecer conexión: {str(e)}"
        }), 503
    except Exception as e:
        Logger.error(f"Error pausando VM: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al procesar la solicitud de pausa",
            "details": str(e)
        }), 500

@app.route('/resume-vm/<vm_id>', methods=['POST'])
def resume_vm_endpoint(vm_id: str):
    """
    Endpoint para reanudar una VM que está pausada.

    Args:
        vm_id (str): ID de la VM a reanudar

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: VM reanudada exitosamente
            400: ID de VM inválido
            404: VM no encontrada o no está pausada
            500: Error interno
            503: Error de comunicación con servidor
    """
    try:
        Logger.major_section(f"API: RESUME VM ID-{vm_id}")
        vm_id = int(vm_id)

        # 0. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 1. Verificar existencia de VM y acceso del usuario
        Logger.debug(f"Verificando acceso del usuario {user_id} a VM {vm_id}")
        access_query = """
            SELECT vm.*, vm.slice as slice_id,
                ps.name as worker_name, ps.id as worker_id, 
                ps.ip as worker_ip,
                ps.ssh_username, ps.ssh_password, ps.ssh_key_path,
                p.user as property_user,
                vm.flavor as flavor_id,  # Agregar el ID del flavor
                vm.image as image_id     # Agregar el ID de la imagen
            FROM virtual_machine vm
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN slice s ON vm.slice = s.id
            JOIN property p ON s.id = p.slice
            WHERE vm.id = %s 
            AND vm.status = 'paused'
            AND p.user = %s"""
        
        vm_info = db.execute_query(access_query, (vm_id, user_id))

        if not vm_info:
            # Verificar si la VM existe
            vm_exists = db.execute_query(
                """SELECT id FROM virtual_machine 
                   WHERE id = %s AND status = 'paused'""",
                (vm_id,)
            )
            
            if not vm_exists:
                Logger.warning(f"VM ID-{vm_id} no encontrada o no está pausada")
                return jsonify({
                    "status": "error",
                    "message": "La máquina virtual no está disponible",
                    "details": "La VM no existe o no se encuentra en estado paused"
                }), 404
            else:
                Logger.warning(f"Usuario {user_id} no tiene acceso a VM {vm_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para reanudar esta máquina virtual"
                }), 403

        vm = vm_info[0]
        Logger.info(f"VM ID-{vm['id']} con nombre '{vm['name']}' encontrada en Worker ID-{vm['worker_id']} con nombre {vm['worker_name']}")

        # 2. Obtener información de flavor e imagen
        Logger.debug("Obteniendo información de recursos")
        flavor_info = db.execute_query(
            "SELECT * FROM flavor WHERE id = %s",
            (vm['flavor_id'],)
        )[0]

        image_info = db.execute_query(
            "SELECT * FROM image WHERE id = %s",
            (vm['image_id'],)
        )[0]

        # 3. Obtener interfaces de la VM
        Logger.debug("Consultando interfaces de la VM")
        interfaces = db.execute_query(
            """SELECT * FROM interface 
               WHERE vm = %s 
               ORDER BY external_access DESC""",
            (vm_id,)
        )
        
        # 4. Preparar datos para Slice Controller
        Logger.debug("Preparando datos para servidor de despliegue")
        resume_data = {
            "vm_info": {
                "id": vm['id'],
                "name": vm['name'],
                "qemu_pid": vm['qemu_pid'],
                "vnc_port": vm['vnc_port'],
                "vnc_display": vm['vnc_display'],
                "status": vm['status'],
                "flavor": {
                    "id": flavor_info['id'],
                    "name": flavor_info['name'],
                    "ram": flavor_info['ram'],
                    "vcpus": flavor_info['vcpus'],
                    "disk": flavor_info['disk']
                },
                "image": {
                    "id": image_info['id'],
                    "name": image_info['name'],
                    "path": image_info['path']
                },
                "interfaces": [
                    {
                        "id": iface['id'],
                        "name": iface['name'],
                        "mac_address": iface['mac'],
                        "tap_name": iface['tap_name'],
                        "external_access": iface['external_access']
                    } for iface in interfaces
                ]
            },
            "worker_info": {
                "id": vm['physical_server'],
                "name": vm['worker_name'],
                "ip": vm['worker_ip'],
                "ssh_username": vm['ssh_username'],
                "ssh_password": vm['ssh_password'],
                "ssh_key_path": vm['ssh_key_path']
            },
            "slice_id": vm['slice_id']
        }
        
        # 5. Enviar solicitud a Slice Controller
        Logger.info("Enviando solicitud de reanudación al servidor de despliegue")
        slice_controller = get_service_instance('slice-controller')
        if not slice_controller:
            raise Exception("Servicio slice-controller no disponible") 

        response = requests.post(
            f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/resume-vm/{vm_id}",
            json=json.loads(json.dumps(resume_data, default=json_handler)),
            timeout=300
        )
        Logger.debug(f"Respuesta recibida: {response.status_code}")

        # 6. Procesar respuesta
        if response.status_code == 200:
            response_data = response.json()
            vm_data = response_data['content']
            
            # 7. Actualizar estado en BD
            Logger.debug("Actualizando estado en base de datos")
            update_query = """
                UPDATE virtual_machine 
                SET status = 'running',
                    qemu_pid = %s,
                    vnc_display = %s,
                    vnc_port = %s
                WHERE id = %s
            """
            db.execute_transaction([(
                update_query, 
                (
                    vm_data.get('qemu_pid'),
                    vm_data.get('vnc_display'),
                    vm_data.get('vnc_port'),
                    vm_id
                )
            )])
            Logger.success(f"VM ID-{vm_id} con nombre '{vm['name']}' reanudada y BD actualizada")

            return jsonify({
                "status": "success",
                "message": f"La máquina virtual con nombre '{vm['name']}' ha sido reanudada exitosamente",
                "content": {
                    "vm_id": vm_id,
                    "name": vm['name'],
                    "qemu_pid": vm_data.get('qemu_pid'),
                    "vnc_display": vm_data.get('vnc_display'),
                    "vnc_port": vm_data.get('vnc_port'),
                    "status": "running"
                }
            }), 200
        else:
            Logger.error(f"Error en servidor de despliegue: {response.text}")
            return jsonify({
                "status": "error",
                "message": "Error al intentar reanudar la máquina virtual",
                "details": response.json().get('message', 'Error desconocido en servidor')
            }), response.status_code

    except ValueError:
        Logger.error(f"ID de VM inválido: ID-{vm_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la máquina virtual es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except requests.RequestException as e:
        Logger.error(f"Error de comunicación con servidor: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error de comunicación con el servidor de despliegue",
            "details": f"No se pudo establecer conexión: {str(e)}"
        }), 503
    except Exception as e:
        Logger.error(f"Error reanudando VM: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error", 
            "message": "Error interno al procesar la solicitud de reanudación",
            "details": str(e)
        }), 500

@app.route('/restart-vm/<vm_id>', methods=['POST'])
def restart_vm_endpoint(vm_id: str):
    """
    Endpoint para reiniciar una VM específica.

    Args:
        vm_id (str): ID de la VM a reiniciar

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: VM reiniciada exitosamente
            400: ID de VM inválido
            404: VM no encontrada o no ejecutándose
            500: Error interno
            503: Error de comunicación con servidor
    """
    try:
        Logger.major_section(f"API: RESTART VM ID-{vm_id}")
        vm_id = int(vm_id)

        # 1. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 2. Verificar existencia de VM y acceso del usuario
        Logger.debug(f"Verificando acceso del usuario {user_id} a VM {vm_id}")
        access_query = """
            SELECT vm.*, vm.slice as slice_id,
                ps.name as worker_name, ps.id as worker_id, 
                ps.ip as worker_ip,
                ps.ssh_username, ps.ssh_password, ps.ssh_key_path,
                p.user as property_user,
                f.name as flavor_name, f.vcpus, f.ram, f.disk
            FROM virtual_machine vm
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN slice s ON vm.slice = s.id
            JOIN property p ON s.id = p.slice
            JOIN flavor f ON vm.flavor = f.id
            WHERE vm.id = %s 
            AND vm.status = 'running'
            AND p.user = %s"""

        vm_info = db.execute_query(access_query, (vm_id, user_id))

        if not vm_info:
            # Verificar si la VM existe
            vm_exists = db.execute_query(
                """SELECT id FROM virtual_machine 
                   WHERE id = %s AND status = 'running'""",
                (vm_id,)
            )
            
            if not vm_exists:
                Logger.warning(f"VM ID-{vm_id} no encontrada o no está en ejecución")
                return jsonify({
                    "status": "error",
                    "message": "La máquina virtual no está disponible",
                    "details": "La VM no existe o no se encuentra en estado running"
                }), 404
            else:
                Logger.warning(f"Usuario {user_id} no tiene acceso a VM {vm_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para reiniciar esta máquina virtual"
                }), 403

        # Continuar con el código existente...
        vm = vm_info[0]
        Logger.info(f"VM ID-{vm['id']} con nombre '{vm['name']}' encontrada en Worker ID-{vm['worker_id']} con nombre {vm['worker_name']}")

        # Obtener interfaces de la VM
        Logger.debug("Consultando interfaces de la VM")
        interfaces = db.execute_query(
            """SELECT id, name, mac as mac_address, ip, 
                      link as link_id, external_access, tap_name
               FROM interface 
               WHERE vm = %s""",
            (vm_id,)
        )

        # 2. Preparar datos para Slice Controller
        Logger.debug("Preparando datos para servidor de despliegue")
        vm_image = f"vm-{vm['id']}-slice-{vm['slice_id']}.qcow2"
        vm_image_path = f"/home/ubuntu/SliceManager/images/{vm_image}"

        restart_data = {
            "vm_info": {
                "id": vm['id'],
                "name": vm['name'],
                "image_path": vm_image_path,
                "status": vm['status'],
                "qemu_pid": vm['qemu_pid'],
                "vnc_display": vm['vnc_display'],
                "flavor": {
                    "name": vm['flavor_name'],
                    "vcpus": vm['vcpus'],
                    "ram": vm['ram'],
                    "disk": vm['disk']
                },
                "interfaces": interfaces
            },
            "worker_info": {
                "id": vm['physical_server'],
                "name": vm['worker_name'],
                "ip": vm['worker_ip'],
                "ssh_username": vm['ssh_username'],
                "ssh_password": vm['ssh_password'],
                "ssh_key_path": vm['ssh_key_path']
            },
            "slice_id": vm['slice_id']
        }

        # 3. Enviar solicitud a Slice Controller
        Logger.info("Enviando solicitud de reinicio al servidor de despliegue")
        slice_controller = get_service_instance('slice-controller')
        if not slice_controller:
            raise Exception("Servicio slice-controller no disponible") 

        response = requests.post(
            f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/restart-vm/{vm_id}",
            json=json.loads(json.dumps(restart_data, default=json_handler)),
            timeout=300
        )
        Logger.debug(f"Respuesta recibida: {response.status_code}")

        # 4. Procesar respuesta
        if response.status_code == 200:
            response_data = response.json()
            vm_data = response_data.get('content', {})
            
            # 5. Actualizar estado en BD
            Logger.debug("Actualizando estado en base de datos")
            update_query = """
                UPDATE virtual_machine 
                SET qemu_pid = %s,
                    status = 'running',
                    vnc_display = %s,
                    vnc_port = %s
                WHERE id = %s
            """
            db.execute_transaction([(
                update_query, 
                (
                    vm_data.get('qemu_pid'),
                    vm_data.get('vnc_display'),
                    vm_data.get('vnc_port'),
                    vm_id
                )
            )])
            Logger.success(f"VM ID-{vm_id} con nombre '{vm['name']}' reiniciada y BD actualizada")

            return jsonify({
                "status": "success",
                "message": f"La máquina virtual con nombre '{vm['name']}' ha sido reiniciada exitosamente",
                "content": {
                    "vm_id": vm_id,
                    "name": vm['name'],
                    "qemu_pid": vm_data.get('qemu_pid'),
                    "vnc_display": vm_data.get('vnc_display'),
                    "vnc_port": vm_data.get('vnc_port'),
                    "status": "running"
                }
            }), 200
        else:
            Logger.error(f"Error en servidor de despliegue: {response.text}")
            return jsonify({
                "status": "error",
                "message": "Error al intentar reiniciar la máquina virtual",
                "details": response.json().get('message', 'Error desconocido en servidor')
            }), response.status_code

    except ValueError:
        Logger.error(f"ID de VM inválido: ID-{vm_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la máquina virtual es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except requests.RequestException as e:
        Logger.error(f"Error de comunicación con servidor: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error de comunicación con el servidor de despliegue",
            "details": f"No se pudo establecer conexión: {str(e)}"
        }), 503
    except Exception as e:
        Logger.error(f"Error reiniciando VM: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al procesar la solicitud de reinicio",
            "details": str(e)
        }), 500

@app.route('/restart-slice/<slice_id>', methods=['POST'])
def restart_slice_endpoint(slice_id: str):
    """
    Endpoint para reiniciar todas las VMs de un slice.

    Args:
        slice_id (str): ID de la slice a reiniciar

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: Slice reiniciado exitosamente
            400: ID de slice inválido
            404: Slice no encontrado o sin VMs activas
            500: Error interno
            503: Error de comunicación con servidor
    """
    try:
        Logger.major_section(f"API: RESTART SLICE ID-{slice_id}")
        slice_id = int(slice_id)

        # 0. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 0.1 Verificar acceso del usuario a través de property
        Logger.debug(f"Verificando acceso del usuario {user_id} al slice {slice_id}")
        access_query = """
            SELECT s.*, p.user as property_user
            FROM slice s
            JOIN property p ON s.id = p.slice
            WHERE s.id = %s AND p.user = %s
        """
        access_check = db.execute_query(access_query, (slice_id, user_id))

        if not access_check:
            # Verificar si el slice existe
            slice_exists = db.execute_query(
                "SELECT id FROM slice WHERE id = %s",
                (slice_id,)
            )
            
            if slice_exists:
                Logger.warning(f"Usuario {user_id} no tiene acceso al slice {slice_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para reiniciar este slice"
                }), 403
            else:
                Logger.warning(f"Slice {slice_id} no encontrado")
                return jsonify({
                    "status": "error",
                    "message": "El slice solicitado no existe",
                    "details": f"No se encontró el slice con ID {slice_id}"
                }), 404

        # 1. Obtener datos de la slice y sus VMs
        Logger.debug("Consultando información de la slice en base de datos")
        query = """
            SELECT 
                s.id, s.name, s.status,
                sn.svlan_id, sn.network, sn.slice_bridge_name,
                sn.patch_port_slice, sn.patch_port_int,
                vm.id as vm_id, vm.name as vm_name, vm.status as vm_status,
                vm.qemu_pid, vm.vnc_port, vm.vnc_display,
                vm.physical_server as worker_id,
                ps.name as worker_name, ps.ip as worker_ip,
                ps.ssh_username, ps.ssh_key_path,
                f.name as flavor_name, f.vcpus, f.ram, f.disk,
                i.path as image_path,
                i.name as image_name
            FROM slice s
            JOIN slice_network sn ON s.id = sn.slice_id
            JOIN virtual_machine vm ON vm.slice = s.id
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN flavor f ON vm.flavor = f.id
            JOIN image i ON vm.image = i.id
            WHERE s.id = %s AND vm.status = 'running'
        """
        result = db.execute_query(query, (slice_id,))
        
        if not result:
            Logger.warning(f"Slice {slice_id} no encontrado o sin VMs en ejecución")
            return jsonify({
                "status": "error",
                "message": "La slice no está disponible para ser reiniciado",
                "details": "No se encontró la slice o no tiene máquinas virtuales en ejecución"
            }), 404

        # 2. Obtener interfaces de las VMs
        Logger.debug("Consultando interfaces de las VMs")
        interfaces_query = """
            SELECT vm, id, name, mac, ip, link, external_access, tap_name
            FROM interface 
            WHERE vm IN (SELECT id FROM virtual_machine WHERE slice = %s)
        """
        interfaces = db.execute_query(interfaces_query, (slice_id,))

        # 3. Procesar y estructurar datos
        Logger.debug("Procesando información obtenida")
        slice_info = {
            "id": result[0]['id'],
            "name": result[0]['name'],
            "status": result[0]['status'],
            "network_config": {
                "svlan_id": result[0]['svlan_id'],
                "network": result[0]['network'],
                "slice_bridge_name": result[0]['slice_bridge_name'],
                "patch_ports": {
                    "slice_side": result[0]['patch_port_slice'],
                    "int_side": result[0]['patch_port_int']
                }
            }
        }

        # Agrupar interfaces por VM
        vm_interfaces = {}
        for iface in interfaces:
            if iface['vm'] not in vm_interfaces:
                vm_interfaces[iface['vm']] = []
            vm_interfaces[iface['vm']].append(iface)

        # Estructurar datos de VMs
        vms = []
        workers = {}
        for row in result:
            vm_ifaces = []
            for iface in vm_interfaces.get(row['vm_id'], []):
                vm_ifaces.append({
                    "id": iface['id'],
                    "name": iface['name'],
                    "mac_address": iface['mac'],
                    "ip": iface['ip'],
                    "link_id": iface['link'],
                    "external_access": bool(iface['external_access']),
                    "tap_name": iface['tap_name']
                })

            vm = {
                "id": row['vm_id'],
                "name": row['vm_name'],
                "status": row['vm_status'],
                "qemu_pid": row['qemu_pid'],
                "vnc_port": row['vnc_port'],
                "vnc_display": row['vnc_display'],
                "physical_server": {"id": row['worker_id']},
                "flavor": {
                    "name": row['flavor_name'],
                    "vcpus": row['vcpus'],
                    "ram": row['ram'],
                    "disk": row['disk']
                },
                "image_path": f"/home/ubuntu/SliceManager/images/vm-{row['vm_id']}-slice-{slice_id}.qcow2",
                "interfaces": vm_ifaces
            }
            vms.append(vm)
            Logger.debug(f"VM procesada: Nombre: {vm['name']}, ID: {vm['id']}")

            if row['worker_id'] not in workers:
                workers[row['worker_id']] = {
                    "id": row['worker_id'],
                    "name": row['worker_name'],
                    "ip": row['worker_ip'],
                    "ssh_username": row['ssh_username'],
                    "ssh_key_path": row['ssh_key_path']
                }
                Logger.debug(f"Worker registrado: {row['worker_name']}")

        # 4. Preparar datos para el request
        Logger.debug("Preparando datos para el servidor de despliegue")
        restart_data = {
            "slice_info": slice_info,
            "vms": vms,
            "workers": workers
        }

        # 5. Enviar request a Slice Controller
        Logger.info("Enviando solicitud de reinicio al servidor de despliegue")
        slice_controller = get_service_instance('slice-controller')
        if not slice_controller:
            raise Exception("Servicio slice-controller no disponible") 

        response = requests.post(
            f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/restart-slice/{slice_id}",
            json=json.loads(json.dumps(restart_data, default=str)),
            timeout=300
        )
        Logger.debug(f"Respuesta recibida: {response.status_code}")

        # 6. Procesar respuesta y actualizar BD
        if response.status_code == 200:
            Logger.info("Reinicio exitoso, actualizando base de datos")
            response_data = response.json()
            
            # Preparar queries de actualización
            update_vm_query = """
                UPDATE virtual_machine 
                SET qemu_pid = %s,
                    status = 'running',
                    vnc_port = %s,
                    vnc_display = %s
                WHERE id = %s
            """
            update_slice_query = """
                UPDATE slice 
                SET status = 'running'
                WHERE id = %s
            """
            
            # Construir transacción
            transactions = [(update_slice_query, (slice_id,))]
            
            # Agregar actualizaciones de VMs
            for vm_data in response_data.get('content', {}).get('vms', []):
                vnc_display = vm_data.get('vnc_display')
                transactions.append((
                    update_vm_query,
                    (
                        vm_data.get('qemu_pid'),
                        5900 + vnc_display if vnc_display else None,
                        vnc_display,
                        vm_data.get('id')
                    )
                ))

            # Ejecutar transacción
            db.execute_transaction(transactions)
            Logger.success(f"Slice ID-{slice_id} reiniciada exitosamente")

            return jsonify({
                "status": "success",
                "message": f"La slice con nombre '{slice_info['name']}' ha sido reiniciada exitosamente",
                "content": response_data.get('content')
            }), 200
        else:
            Logger.error(f"Error en servidor de despliegue: {response.text}")
            return jsonify({
                "status": "error",
                "message": "Error al intentar reiniciar la slice",
                "details": response.json().get('message', 'Error desconocido en servidor')
            }), response.status_code

    except ValueError:
        Logger.error(f"ID de slice inválido: {slice_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la slice es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except requests.RequestException as e:
        Logger.error(f"Error de comunicación con servidor: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error de comunicación con el servidor de despliegue",
            "details": f"No se pudo establecer conexión: {str(e)}"
        }), 503
    except Exception as e:
        Logger.error(f"Error reiniciando slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al procesar la solicitud de reinicio",
            "details": str(e)
        }), 500

@app.route('/stop-slice/<slice_id>', methods=['POST'])
def stop_slice_endpoint(slice_id: str):
    """
    Endpoint para detener todas las VMs de un slice.

    Args:
        slice_id (str): ID de la slice a detener

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: Slice detenido exitosamente
            400: ID de slice inválido
            404: Slice no encontrado o sin VMs activas
            500: Error interno
            503: Error de comunicación con servidor
    """
    try:
        Logger.major_section(f"API: STOP SLICE ID-{slice_id}")
        slice_id = int(slice_id)

        # 0. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 0.1 Verificar acceso del usuario a través de property
        Logger.debug(f"Verificando acceso del usuario {user_id} al slice {slice_id}")
        access_query = """
            SELECT s.*, p.user as property_user
            FROM slice s
            JOIN property p ON s.id = p.slice
            WHERE s.id = %s AND p.user = %s
        """
        access_check = db.execute_query(access_query, (slice_id, user_id))

        if not access_check:
            # Verificar si el slice existe
            slice_exists = db.execute_query(
                "SELECT id FROM slice WHERE id = %s",
                (slice_id,)
            )
            
            if slice_exists:
                Logger.warning(f"Usuario {user_id} no tiene acceso al slice {slice_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para detener este slice"
                }), 403
            else:
                Logger.warning(f"Slice {slice_id} no encontrado")
                return jsonify({
                    "status": "error",
                    "message": "El slice solicitado no existe",
                    "details": f"No se encontró el slice con ID {slice_id}"
                }), 404
            
        # 1. Obtener información de la slice y sus VMs
        Logger.debug("Consultando información de la slice en base de datos")
        query = """
            SELECT 
                s.id, s.name, s.status,
                vm.id as vm_id, vm.name as vm_name, 
                vm.status as vm_status, vm.qemu_pid,
                ps.id as worker_id, ps.name as worker_name, 
                ps.ip as worker_ip,
                ps.ssh_username, ps.ssh_password, ps.ssh_key_path
            FROM slice s
            JOIN virtual_machine vm ON vm.slice = s.id
            JOIN physical_server ps ON vm.physical_server = ps.id
            WHERE s.id = %s AND vm.status = 'running'
        """
        
        result = db.execute_query(query, (slice_id,))
        
        if not result:
            Logger.warning(f"Slice ID-{slice_id} no encontrado o sin VMs en ejecución")
            return jsonify({
                "status": "error",
                "message": "La slice no está disponible para ser detenida",
                "details": "No se encontró la slice o no tiene máquinas virtuales en ejecución"
            }), 404
        

        slice_network_info = db.execute_query(
            """SELECT s.*, sn.* 
            FROM slice s
            JOIN slice_network sn ON s.id = sn.slice_id
            WHERE s.id = %s""",
            (slice_id,)
        )

        # 2. Preparar datos para el request
        Logger.debug("Estructurando datos para detener slice")
        stop_data = {
            "slice_info": {
                "id": result[0]['id'],
                "name": result[0]['name'],
                "status": result[0]['status']
            },
            "vms": [],
            "workers": {},
            "network_config":{
                "slice_id": slice_id,
                "svlan_id": slice_network_info[0]['svlan_id'],
                "network": slice_network_info[0]['network'],
                "dhcp_range": [
                    slice_network_info[0]['dhcp_range_start'],
                    slice_network_info[0]['dhcp_range_end']
                ],
                "slice_bridge_name": slice_network_info[0]['slice_bridge_name'],
                "patch_ports": {
                    "slice_side": slice_network_info[0]['patch_port_slice'],
                    "int_side": slice_network_info[0]['patch_port_int']
                },
                "dhcp_interface": slice_network_info[0]['dhcp_interface'],
                "gateway_interface": slice_network_info[0]['gateway_interface']
            }
        }

        # Procesar información de VMs y workers
        for row in result:
            # Agregar VM con su worker asociado
            vm_data = {
                "id": row['vm_id'],
                "name": row['vm_name'],
                "status": row['vm_status'],
                "qemu_pid": row['qemu_pid'],
                "physical_server": {
                    "id": row['worker_id'],
                    "name": row['worker_name']
                }
            }
            stop_data['vms'].append(vm_data)
            Logger.debug(f"VM procesada: Nombre: {vm_data['name']}, ID: {vm_data['id']}")

            # Agregar worker si no existe
            if str(row['worker_id']) not in stop_data['workers']:
                stop_data['workers'][str(row['worker_id'])] = {
                    "name": row['worker_name'],
                    "ip": row['worker_ip'],
                    "ssh_username": row['ssh_username'],
                    "ssh_password": row['ssh_password'],
                    "ssh_key_path": row['ssh_key_path']
                }
                Logger.debug(f"Worker registrado: {row['worker_name']}")

        # 3. Enviar request a Slice Controller
        Logger.info("Enviando solicitud de detención al servidor de despliegue")
        slice_controller = get_service_instance('slice-controller')
        if not slice_controller:
            raise Exception("Servicio slice-controller no disponible") 

        response = requests.post(
            f"http://{slice_controller['ipAddr']}:{slice_controller['port']}/stop-slice/{slice_id}",
            json=stop_data,
            timeout=300
        )
        Logger.debug(f"Respuesta recibida: {response.status_code}")

        # 4. Procesar respuesta y actualizar BD
        if response.status_code == 200:
            Logger.info("Detención exitosa, actualizando base de datos")
            
            # Preparar queries de actualización
            update_vm_query = """
                UPDATE virtual_machine 
                SET status = 'stopped',
                    qemu_pid = NULL,
                    vnc_port = NULL,
                    vnc_display = NULL
                WHERE id = %s
            """
            
            update_slice_query = """
                UPDATE slice 
                SET status = 'stopped'
                WHERE id = %s
            """
            
            # Construir transacción
            transactions = [(update_slice_query, (slice_id,))]
            
            # Agregar actualizaciones de VMs
            for vm in stop_data['vms']:
                transactions.append((
                    update_vm_query, 
                    (vm['id'],)
                ))

            # Ejecutar transacción
            db.execute_transaction(transactions)
            Logger.success(f"Slice ID-{slice_id} detenida exitosamente")

            return jsonify({
                "status": "success",
                "message": f"La slice con nombre '{stop_data['slice_info']['name']}' ha sido detenida exitosamente",
                "content": {
                    "slice_id": slice_id,
                    "vms_stopped": len(stop_data['vms'])
                }
            }), 200
        else:
            Logger.error(f"Error en servidor de despliegue: {response.text}")
            return jsonify({
                "status": "error",
                "message": "Error al intentar detener la slice",
                "details": response.json().get('message', 'Error desconocido en servidor')
            }), response.status_code

    except ValueError:
        Logger.error(f"ID de slice inválido: {slice_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la slice es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except requests.RequestException as e:
        Logger.error(f"Error de comunicación con servidor: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Error de comunicación con el servidor de despliegue",
            "details": f"No se pudo establecer conexión: {str(e)}"
        }), 503
    except Exception as e:
        Logger.error(f"Error deteniendo slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al procesar la solicitud de detención",
            "details": str(e)
        }), 500

@app.route('/slice/<slice_id>', methods=['GET'])
def get_slice(slice_id):
    """
    Obtiene información completa de un slice desde la BD.

    Args:
        slice_id (str): ID del la slice a consultar

    Returns:
        Response: Información completa de la slice incluyendo:
            - Información básica de la slice
            - Configuración de red
            - VMs con sus recursos asignados
            - Links y sus configuraciones
            - Interfaces de red
    """
    try:
        Logger.major_section(f"API: GET SLICE ID-{slice_id}")
        slice_id = int(slice_id)
        
        # 0. Validar al usuario y su relación con la slice
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # Verificar acceso del usuario a través de la tabla property
        Logger.debug(f"Verificando acceso del usuario {user_id} al slice {slice_id}")
        access_check = db.execute_query(
            """SELECT p.*, u.username 
               FROM property p
               JOIN user u ON p.user = u.id
               WHERE p.slice = %s AND p.user = %s""",
            (slice_id, user_id)
        )

        if not access_check:
            # Verificar si el slice existe
            slice_exists = db.execute_query(
                "SELECT id FROM slice WHERE id = %s",
                (slice_id,)
            )
            
            if slice_exists:
                Logger.warning(f"Usuario {user_id} no tiene acceso al slice {slice_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para acceder a este slice"
                }), 403
            else:
                Logger.warning(f"Slice {slice_id} no encontrado")
                return jsonify({
                    "status": "error",
                    "message": "El slice solicitado no existe",
                    "details": f"No se encontró el slice con ID {slice_id}"
                }), 404

        # 1. Obtener información básica de la slice y recursos totales
        Logger.debug("Consultando información básica y recursos de la slice")
        slice_info = db.execute_query(
            """SELECT s.*, sn.*,
                COALESCE(SUM(f.vcpus), 0) as total_vcpus,
                COALESCE(SUM(f.ram), 0) as total_ram,
                COALESCE(SUM(f.disk), 0) as total_disk,
                COUNT(DISTINCT vm.id) as vm_count
            FROM slice s
            JOIN slice_network sn ON s.id = sn.slice_id
            LEFT JOIN virtual_machine vm ON s.id = vm.slice
            LEFT JOIN flavor f ON vm.flavor = f.id
            WHERE s.id = %s
            GROUP BY s.id, sn.slice_id""",
            (slice_id,)
        )

        if not slice_info:
            Logger.warning(f"No se encontró la Slice ID-{slice_id}")
            return jsonify({
                "status": "error",
                "message": "La slice solicitado no existe",
                "details": f"No se encontró la slice con ID-{slice_id}"
            }), 404

        # 2. Obtener VMs con sus workers y recursos
        Logger.debug("Consultando VMs y recursos asignados")
        vms = db.execute_query(
            """SELECT DISTINCT 
                vm.id as vm_id,
                vm.name as vm_name,
                vm.image, vm.flavor, 
                vm.slice as slice_id,
                vm.physical_server,
                vm.status,
                vm.vnc_port, vm.vnc_display, 
                vm.qemu_pid,
                ps.name as worker_name,
                f.name as flavor_name,
                f.ram, f.vcpus, f.disk,
                i.name as image_name
            FROM virtual_machine vm
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN flavor f ON vm.flavor = f.id
            JOIN image i ON vm.image = i.id
            WHERE vm.slice = %s
            ORDER BY vm.id""",
            (slice_id,)
        )
        Logger.debug(f"VMs encontradas: {len(vms)}")

        # 3. Obtener links de red
        Logger.debug("Consultando links de red")
        links = db.execute_query(
            "SELECT * FROM link WHERE slice_id = %s",
            (slice_id,)
        )
        Logger.debug(f"Links encontrados: {len(links)}")

        # 4. Obtener interfaces de red
        Logger.debug("Consultando interfaces de red")
        Logger.debug("Consultando interfaces de red")
        interfaces = db.execute_query(
            """SELECT i.* 
            FROM interface i
            JOIN virtual_machine vm ON i.vm = vm.id
            WHERE vm.slice = %s
            ORDER BY i.external_access DESC""",  # Ordenar por external_access descendente (1 antes que 0)
            (slice_id,)
        )
        Logger.debug(f"Interfaces encontradas: {len(interfaces)}")

        # 5. Construir respuesta estructurada
        Logger.debug("Estructurando respuesta")
        slice_data = {
            "slice_info": {
                "id": slice_id, 
                "name": slice_info[0]['name'],
                "description": slice_info[0]['description'],
                "status": slice_info[0]['status'],
                "created_at": slice_info[0]['created_at'].isoformat() if slice_info[0]['created_at'] else None
            },
            "resources": {
                "vcpus": slice_info[0]['total_vcpus'],
                "ram": slice_info[0]['total_ram'],
                "disk": slice_info[0]['total_disk'],
                "vm_count": slice_info[0]['vm_count']
            },
            "network_config": {
                "slice_id": slice_id,
                "svlan_id": slice_info[0]['svlan_id'],
                "network": slice_info[0]['network'],
                "dhcp_range": [
                    slice_info[0]['dhcp_range_start'],
                    slice_info[0]['dhcp_range_end']
                ],
                "slice_bridge_name": slice_info[0]['slice_bridge_name'],
                "patch_ports": {
                    "slice_side": slice_info[0]['patch_port_slice'],
                    "int_side": slice_info[0]['patch_port_int']
                },
                "dhcp_interface": slice_info[0]['dhcp_interface'],
                "gateway_interface": slice_info[0]['gateway_interface']
            },
            "topology_info": {
                "vms": [{
                    "id": vm['vm_id'],
                    "name": vm['vm_name'],
                    "status": vm['status'],
                    "image_id": vm['image'],
                    "flavor_id": vm['flavor'],
                    "physical_server": {
                        "id": vm['physical_server'],
                        "name": vm['worker_name']
                    },
                    "vnc_display": vm['vnc_display'],
                    "vnc_port": vm['vnc_port'],
                    "qemu_pid": vm['qemu_pid']
                } for vm in vms],
                "links": [{
                    "id": link['id'],
                    "name": link['name'],
                    "cvlan_id": link['cvlan_id']
                } for link in links],
                "interfaces": [{
                    "id": iface['id'],
                    "name": iface['name'],
                    "vm_id": iface['vm'],
                    "link_id": iface['link'],
                    "mac_address": iface['mac'],
                    "ip": iface['ip'],
                    "external_access": iface['external_access'],
                    "tap_name": iface['tap_name']
                } for iface in interfaces]
            }
        }
        
        Logger.success(f"Información del la Slice ID-{slice_id} obtenida exitosamente")
        return jsonify({
            "status": "success",
            "message": f"Información de la slice ID-{slice_id} con nombre '{slice_info[0]['name']}' obtenida exitosamente",
            "content": slice_data
        }), 200

    except ValueError:
        Logger.error(f"ID de slice inválido: ID-{slice_id}")
        return jsonify({
            "status": "error",
            "message": "El ID del la slice es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except Exception as e:
        Logger.error(f"Error obteniendo slice: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al obtener información de la slice",
            "details": str(e)
        }), 500

@app.route('/vm/<vm_id>', methods=['GET'])
def get_vm(vm_id):
    """
    Obtiene información detallada de una máquina virtual específica.
    
    Args:
        vm_id (str): ID de la VM a consultar
        

    Returns:
        Response: Información completa de la VM incluyendo:
            - Información básica de la VM
            - Recursos asignados (flavor)
            - Imagen base
            - Worker donde está desplegada
            - Interfaces de red
            - Estado actual y configuración VNC
            
        Códigos de respuesta:
            200: VM encontrada y acceso permitido
            400: ID inválido o falta user_id
            403: Usuario no autorizado
            404: VM no encontrada
            500: Error interno
    """
    try:
        Logger.major_section(f"API: GET VM ID-{vm_id}")
        vm_id = int(vm_id)

        # 1. Validar user_id 
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 2. Verificar VM y acceso del usuario mediante property
        Logger.debug(f"Verificando acceso del usuario {user_id} a VM {vm_id}")
        access_query = """
            SELECT 
                vm.*,
                s.id as slice_id, s.name as slice_name,
                p.user as property_user,
                ps.name as worker_name, ps.ip as worker_ip,
                f.name as flavor_name, f.vcpus, f.ram, f.disk,
                i.name as image_name, i.path as image_path,
                u.username
            FROM virtual_machine vm
            JOIN slice s ON vm.slice = s.id
            JOIN property p ON s.id = p.slice
            JOIN physical_server ps ON vm.physical_server = ps.id
            JOIN flavor f ON vm.flavor = f.id
            JOIN image i ON vm.image = i.id
            JOIN user u ON p.user = u.id
            WHERE vm.id = %s AND p.user = %s
        """

        vm_info = db.execute_query(access_query, (vm_id, user_id))

        if not vm_info:
            # Verificar si la VM existe
            vm_exists = db.execute_query(
                "SELECT id FROM virtual_machine WHERE id = %s",
                (vm_id,)
            )
            
            if vm_exists:
                Logger.warning(f"Usuario {user_id} no tiene acceso a VM {vm_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para acceder a esta máquina virtual"
                }), 403
            else:
                Logger.warning(f"VM {vm_id} no encontrada")
                return jsonify({
                    "status": "error",
                    "message": "La máquina virtual no existe",
                    "details": f"No se encontró la VM con ID {vm_id}"
                }), 404

        vm = vm_info[0]
        Logger.info(f"VM encontrada: ID-{vm_id}, Nombre: {vm['name']}")

        # 3. Obtener interfaces de red
        Logger.debug("Consultando interfaces de red")
        interfaces_query = """
            SELECT id, name, mac, ip, link, external_access, tap_name
            FROM interface 
            WHERE vm = %s
            ORDER BY external_access DESC, id
        """
        interfaces = db.execute_query(interfaces_query, (vm_id,))
        Logger.debug(f"Interfaces encontradas: {len(interfaces)}")

        # 4. Estructurar respuesta
        vm_data = {
            "vm_info": {
                "id": vm['id'],
                "name": vm['name'],
                "status": vm['status'],
                "qemu_pid": vm['qemu_pid'],
                "vnc_display": vm['vnc_display'],
                "vnc_port": vm['vnc_port']
            },
            "slice": {
                "id": vm['slice_id'],
                "name": vm['slice_name']
            },
            "resources": {
                "flavor": {
                    "id": vm['flavor'],
                    "name": vm['flavor_name'],
                    "vcpus": vm['vcpus'],
                    "ram": vm['ram'],
                    "disk": vm['disk']
                },
                "image": {
                    "id": vm['image'],
                    "name": vm['image_name'],
                    "path": vm['image_path']
                }
            },
            "worker": {
                "id": vm['physical_server'],
                "name": vm['worker_name'],
                "ip": vm['worker_ip']
            },
            "interfaces": [{
                "id": iface['id'],
                "name": iface['name'],
                "mac_address": iface['mac'],
                "ip": iface['ip'],
                "link_id": iface['link'],
                "external_access": bool(iface['external_access']),
                "tap_name": iface['tap_name']
            } for iface in interfaces],
            "user": {
                "id": int(user_id),
                "username": vm['username']
            }
        }

        Logger.success(f"Información de VM ID-{vm_id} obtenida exitosamente")
        return jsonify({
            "status": "success",
            "message": f"Información de la VM '{vm['name']}' obtenida exitosamente",
            "content": vm_data
        }), 200

    except ValueError:
        Logger.error(f"ID de VM inválido: {vm_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la máquina virtual es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except Exception as e:
        Logger.error(f"Error obteniendo VM: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al obtener información de la VM",
            "details": str(e)
        }), 500

@app.route('/list-slices', methods=['GET'])
def list_slices():
    """
    Obtiene todos los slices a los que tiene acceso un usuario.
    
    Query params:
        

    Returns:
        Response: Lista de slices con recursos y estado
            200: Slices obtenidos exitosamente
            400: user_id no proporcionado
            500: Error interno
    """
    try:
        Logger.major_section("API: LIST USER SLICES")
        
        # Obtener y validar token
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # Consultar slices del usuario a través de la tabla property
        Logger.debug(f"Consultando slices para usuario ID-{user_id}")
        query = """
            SELECT DISTINCT
                s.id,
                s.name,
                s.description,
                s.status,
                s.created_at,
                COALESCE(SUM(f.vcpus), 0) as total_vcpus,
                COALESCE(SUM(f.ram), 0) as total_ram,
                COALESCE(SUM(f.disk), 0) as total_disk,
                COUNT(vm.id) as vm_count,
                u.username,
                u.name as user_name,
                u.lastname as user_lastname
            FROM slice s
            JOIN property p ON s.id = p.slice
            JOIN user u ON p.user = u.id
            LEFT JOIN virtual_machine vm ON s.id = vm.slice
            LEFT JOIN flavor f ON vm.flavor = f.id
            WHERE p.user = %s
            GROUP BY s.id, s.name, s.description, s.status, s.created_at,
                     u.username, u.name, u.lastname
            ORDER BY s.created_at DESC
        """
        
        slices = db.execute_query(query, (user_id,))
        Logger.debug(f"Slices encontrados: {len(slices)}")

        # Formatear respuesta
        formatted_slices = []
        for slice_data in slices:
            formatted_slice = {
                "id": slice_data['id'],
                "name": slice_data['name'],
                "description": slice_data['description'],
                "status": slice_data['status'],
                "created_at": slice_data['created_at'].isoformat() if slice_data['created_at'] else None,
                "resources": {
                    "vcpus": slice_data['total_vcpus'],
                    "ram": slice_data['total_ram'],
                    "disk": slice_data['total_disk'],
                    "vm_count": slice_data['vm_count']
                },
                "user": {
                    "id": int(user_id),
                    "username": slice_data['username'],
                    "name": slice_data['user_name'],
                    "lastname": slice_data['user_lastname']
                }
            }
            formatted_slices.append(formatted_slice)
            Logger.debug(
                f"Slice procesado: ID-{formatted_slice['id']}, "
                f"Nombre: '{formatted_slice['name']}', "
                f"VMs: {formatted_slice['resources']['vm_count']}"
            )

        Logger.success(f"Slices obtenidos exitosamente para usuario ID-{user_id}")
        return jsonify({
            "status": "success",
            "message": f"Se encontraron {len(formatted_slices)} slices para el usuario",
            "content": formatted_slices
        }), 200

    except Exception as e:
        Logger.error(f"Error listando slices: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al listar slices",
            "details": str(e)
        }), 500

@app.route('/resources/flavors', methods=['GET'])
def get_flavors():
    """
    Obtiene todos los flavors activos del sistema.

    Returns:
        Response: Lista de flavors con sus especificaciones
            200: Flavors obtenidos exitosamente
            500: Error interno del servidor
    """
    try:
        Logger.major_section("API: GET FLAVORS")
        
        # Obtener user_id 
        user_id = request.args.get('user_id')
        Logger.debug(f"User ID: {user_id if user_id else 'No proporcionado'}")
        
        # Construir query base
        base_query = """
            SELECT id, name, vcpus, ram, disk, type, state, user
            FROM flavor 
            WHERE state = 'active' 
            AND (user IS NULL"""  # Recursos públicos
            
        # Agregar condición para recursos privados si hay user_id
        if user_id:
            base_query += f" OR user = {user_id})"  # Recursos privados del usuario
        else:
            base_query += ")"  # Solo recursos públicos
            
        base_query += " ORDER BY id"
        
        # Consultar flavors
        Logger.debug("Consultando flavors en base de datos")
        flavors = db.execute_query(base_query)
        Logger.debug(f"Flavors encontrados: {len(flavors)}")

        # Formatear respuesta
        formatted_flavors = [{
            'id': flavor['id'],
            'name': flavor['name'],
            'vcpus': flavor['vcpus'],
            'ram': flavor['ram'],
            'disk': flavor['disk'],
            'type': flavor['type']
        } for flavor in flavors]

        Logger.success("Flavors obtenidos exitosamente")
        return jsonify({
            "status": "success",
            "message": "Los tipos de máquinas virtuales fueron obtenidos exitosamente",
            "content": formatted_flavors
        }), 200

    except Exception as e:
        Logger.error(f"Error obteniendo flavors: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error al obtener los tipos de máquinas virtuales",
            "details": str(e)
        }), 500

@app.route('/resources/images', methods=['GET'])
def get_images():
    """
    Obtiene todas las imágenes activas del sistema.

    Returns:
        Response: Lista de imágenes disponibles
            200: Imágenes obtenidas exitosamente
            500: Error interno del servidor
    """
    try:
        Logger.major_section("API: GET IMAGES")
        
        # Obtener user_id
        user_id = request.args.get('user_id')
        Logger.debug(f"User ID: {user_id if user_id else 'No proporcionado'}")
        
        # Construir query base
        base_query = """
            SELECT id, name, path, type, state, user
            FROM image
            WHERE state = 'active' 
            AND (user IS NULL"""  # Recursos públicos
            
        # Agregar condición para recursos privados si hay user_id
        if user_id:
            base_query += f" OR user = {user_id})"  # Recursos privados del usuario
        else:
            base_query += ")"  # Solo recursos públicos
            
        base_query += " ORDER BY id"
        
        # Consultar imágenes
        Logger.debug("Consultando imágenes en base de datos")
        images = db.execute_query(base_query)

        Logger.debug(f"Imágenes encontradas: {len(images)}")

        # Formatear respuesta
        formatted_images = [{
            'id': image['id'],
            'name': image['name'],
            'path': image['path'],
            'type': image['type']
        } for image in images]

        Logger.success("Imágenes obtenidas exitosamente")
        return jsonify({
            "status": "success",
            "message": "Las imágenes de sistema operativo fueron obtenidas exitosamente",
            "content": formatted_images
        }), 200

    except Exception as e:
        Logger.error(f"Error obteniendo imágenes: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error al obtener las imágenes de sistema operativo",
            "details": str(e)
        }), 500

@app.route('/get-available-vnc-displays', methods=['POST'])
def get_available_vnc_displays():
    """
    Obtiene los displays VNC disponibles para los workers especificados.
    
    Request body:
        {"worker_ids": [int]} - Lista de IDs de workers
        
    Returns:
        Response: Displays disponibles por worker
            200: Displays obtenidos exitosamente
            400: Request inválido
            500: Error interno
    """
    try:
        Logger.major_section("API: GET AVAILABLE VNC DISPLAYS")
        request_data = request.get_json()
        worker_ids = request_data.get('worker_ids', [])

        if not worker_ids:
            Logger.error("No se proporcionaron IDs de workers")
            return jsonify({
                "status": "error",
                "message": "No se proporcionaron IDs de workers",
                "details": "El request debe incluir un arreglo 'worker_ids' no vacío"
            }), 400

        # Obtener displays usados por cada worker
        Logger.debug(f"Procesando {len(worker_ids)} workers")
        displays_by_worker = {}
        
        for worker_id in worker_ids:
            Logger.debug(f"Consultando displays para worker {worker_id}")
            query = """
                SELECT vnc_display 
                FROM virtual_machine 
                WHERE physical_server = %s 
                AND status = 'running'
                AND vnc_display IS NOT NULL
                ORDER BY vnc_display
            """
            used_displays = db.execute_query(query, (worker_id,))
            Logger.debug(f"Displays en uso: {len(used_displays)}")
            
            # Buscar displays disponibles (rango 1-100)
            used_numbers = {d['vnc_display'] for d in used_displays if d['vnc_display']}
            available = []
            current = 1
            
            while current <= 100:  # Límite de displays 1-100
                if current not in used_numbers:
                    available.append(current)
                current += 1

            displays_by_worker[str(worker_id)] = available
            Logger.debug(f"Worker {worker_id}: {len(available)} displays disponibles")

        Logger.success("Displays VNC obtenidos exitosamente")
        return jsonify({
            "status": "success",
            "message": "Los displays VNC disponibles fueron obtenidos exitosamente",
            "content": displays_by_worker
        }), 200

    except Exception as e:
        Logger.error(f"Error obteniendo displays VNC: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error al obtener los displays VNC disponibles",
            "details": str(e)
        }), 500

# @app.route('/vm-vnc/<vm_id>')
# def vm_vnc(vm_id):
#     """
#     Endpoint para acceder a la consola VNC de una VM.
    
#     Args:
#         vm_id (str): ID de la VM
#         token (query param): Token JWT de autenticación
        
#     Returns:
#         Response: Página HTML con cliente VNC o error
#             200: Template VNC renderizado
#             403: Token inválido
#             404: VM no encontrada
#             500: Error interno
#     """
#     try:
#         Logger.major_section(f"VNC CONSOLE VM ID-{vm_id}")
        
#         # Verificar token
#         Logger.debug("Validando token de acceso")
#         Logger.debug(f"Request: {request.args}")
#         token = request.args.get('token')
#         Logger.debug(f"Token: {token}")
#         if not token or not vnc_token_manager.validate_token(token, int(vm_id)):
#             Logger.warning(f"Token inválido o expirado para VM ID-{vm_id}")
#             return jsonify({
#                 "status": "error",
#                 "message": "Acceso denegado",
#                 "details": "El token de acceso es inválido o ha expirado"
#             }), 403
            
#         # Obtener información de la VM
#         Logger.debug("Consultando información de VM")
#         vm_info = db.execute_query(
#             """SELECT vm.*, ps.name as worker_name 
#                FROM virtual_machine vm
#                JOIN physical_server ps ON vm.physical_server = ps.id
#                WHERE vm.id = %s""",
#             (vm_id,)
#         )
        
#         if not vm_info:
#             Logger.warning(f"VM ID-{vm_id} no encontrada")
#             return jsonify({
#                 "status": "error",
#                 "message": "La máquina virtual solicitada no existe",
#                 "details": f"No se encontró la VM con ID {vm_id}"
#             }), 404
            
#         vm = vm_info[0]
#         Logger.info(f"Renderizando cliente VNC para VM ID-{vm_id} con nombre '{vm['name']}'")
        
#         return render_template('vnc.html', 
#             vm_id=vm_id, 
#             vm_name=vm['name'], 
#             token=token
#         )
        
#     except Exception as e:
#         Logger.error(f"Error accediendo a VNC: {str(e)}")
#         Logger.debug(f"Traceback: {traceback.format_exc()}")
#         return jsonify({
#             "status": "error",
#             "message": f"Error al acceder a la consola VNC de la VM con nombre '{vm['name']}'",
#             "details": str(e)
#         }), 500

@app.route('/vm-token/<vm_id>', methods=['POST'])
def generate_vnc_token(vm_id):
    """
    Genera un token JWT para acceso VNC a una VM.
    
    Args:
        vm_id (str): ID de la VM para la cual generar el token
        
    Returns:
        Response: Token JWT y URL de acceso
            200: Token generado exitosamente
            400: ID de VM inválido
            404: VM no encontrada o no activa
            500: Error interno
    """
    try:
        Logger.major_section(f"API: GENERATE VNC TOKEN VM ID-{vm_id}")
        vm_id = int(vm_id)

        # 1. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 2. Verificar VM y acceso del usuario
        Logger.debug(f"Verificando acceso del usuario {user_id} a VM {vm_id}")
        access_query = """
            SELECT vm.*, s.id as slice_id, s.name as slice_name,
                   p.user as property_user
            FROM virtual_machine vm
            JOIN slice s ON vm.slice = s.id
            JOIN property p ON s.id = p.slice
            WHERE vm.id = %s 
            AND vm.status = 'running'
            AND p.user = %s"""

        vm_info = db.execute_query(access_query, (vm_id, user_id))

        if not vm_info:
            # Verificar si la VM existe y está running
            vm_exists = db.execute_query(
                """SELECT id FROM virtual_machine 
                   WHERE id = %s AND status = 'running'""",
                (vm_id,)
            )
            
            if not vm_exists:
                Logger.warning(f"VM ID-{vm_id} no encontrada o no está activa")
                return jsonify({
                    "status": "error",
                    "message": "La máquina virtual no está disponible",
                    "details": "La VM no existe o no se encuentra en ejecución"
                }), 404
            else:
                Logger.warning(f"Usuario {user_id} no tiene acceso a VM {vm_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para acceder a esta máquina virtual"
                }), 403

        vm = vm_info[0]
        Logger.info(f"VM encontrada: Nombre: {vm['name']}, ID: {vm_id}, Slice-ID: {vm['slice_id']}, Slice-Name: {vm['slice_name']}")
        
        # Generar token JWT
        Logger.debug("Generando token JWT")
        token = vnc_token_manager.generate_token(vm_id)
        Logger.debug("Token generado exitosamente")
        
        # Construir URL de acceso
        vnc_url = f"/vm-vnc/{vm_id}?token={token}"
        Logger.success(f"Token VNC generado para VM ID-{vm_id} con nombre '{vm['name']}'")
        
        return jsonify({
            "status": "success",
            "message": "Token de acceso VNC generado exitosamente para la VM solicitada",
            "details": "El token es válido por 10 minutos",
            "content": {
                "vm_id": vm_id,
                "vm_name": vm['name'],
                "slice_id": vm['slice_id'],
                "token": token,
                "url": vnc_url
            }
        }), 200
        
    except ValueError:
        Logger.error(f"ID de VM inválido: ID-{vm_id}")
        return jsonify({
            "status": "error",
            "message": "El ID de la máquina virtual es inválido",
            "details": "El ID debe ser un número entero"
        }), 400
    except Exception as e:
        Logger.error(f"Error generando token VNC: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al generar token de acceso VNC",
            "details": str(e)
        }), 500

@sock.route('/vnc-socket/<vm_id>')
def vnc_proxy(ws, vm_id):
    """
    WebSocket proxy para conexión VNC a una VM específica.
    
    Args:
        ws: WebSocket connection object
        vm_id (str): ID de la VM a conectar
        
    Notes:
        - Requiere token JWT válido en query params
        - Establece túnel bidireccional entre WebSocket y socket VNC
        - Usa threads separados para cada dirección del túnel
    """
    try:
        Logger.major_section(f"API: SOCKET VNC CONSOLE VM ID-{vm_id}")
        
        # 1. Verificar token
        Logger.debug("Validando token de acceso")
        token = request.args.get('token')
        if not token or not vnc_token_manager.validate_token(token, int(vm_id)):
            Logger.warning(f"Token inválido o expirado para VM ID-{vm_id}")
            ws.send("Acceso denegado: Token inválido o expirado")
            ws.close(1008, "Token inválido")
            return

        # 2. Obtener información de la VM
        Logger.debug("Consultando información de VM y worker")
        vm_info = db.execute_query(
            """SELECT vm.*, ps.data_ip, ps.name
               FROM virtual_machine vm
               JOIN physical_server ps ON vm.physical_server = ps.id
               WHERE vm.id = %s AND vm.status = 'running'""",
            (vm_id,)
        )
        
        if not vm_info:
            Logger.warning(f"VM ID-{vm_id} no encontrada o no activa")
            ws.send("VM no disponible para conexión VNC")
            ws.close(1008, "VM no disponible")
            return

        # 3. Establecer conexión VNC
        vm = vm_info[0]
        worker_ip = vm['data_ip']
        vnc_port = vm['vnc_port']
        
        Logger.info(f"Conectando a VNC en {worker_ip}:{vnc_port}")
        vnc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        vnc_socket.settimeout(10)
        
        try:
            vnc_socket.connect((worker_ip, int(vnc_port)))
            vnc_socket.settimeout(None)
        except Exception as e:
            Logger.error(f"Error conectando a servidor VNC: {str(e)}")
            ws.send(f"Error de conexión: {str(e)}")
            ws.close(1011, "Error de conexión")
            return
            
        Logger.success(f"Conexión VNC establecida con {worker_ip}:{vnc_port}")
        stop_event = threading.Event()
        
        # 4. Definir funciones de proxy
        def ws_to_vnc():
            """Reenvía datos desde WebSocket hacia VNC"""
            try:
                while not stop_event.is_set():
                    message = ws.receive(timeout=1)
                    if message is None:
                        continue
                    vnc_socket.sendall(message)
            except Exception as e:
                Logger.error(f"Error en ws_to_vnc: {str(e)}")
            finally:
                stop_event.set()
                
        def vnc_to_ws():
            """Reenvía datos desde VNC hacia WebSocket"""
            try:
                vnc_socket.setblocking(False)
                while not stop_event.is_set():
                    readable, _, _ = select.select([vnc_socket], [], [], 1)
                    if vnc_socket in readable:
                        data = vnc_socket.recv(4096)
                        if not data:
                            break
                        ws.send(data)
            except Exception as e:
                Logger.error(f"Error en vnc_to_ws: {str(e)}")
            finally:
                stop_event.set()
                
        # 5. Iniciar threads de proxy
        Logger.debug("Iniciando threads de proxy")
        ws_thread = threading.Thread(target=ws_to_vnc)
        vnc_thread = threading.Thread(target=vnc_to_ws)
        
        ws_thread.daemon = True
        vnc_thread.daemon = True
        
        ws_thread.start()
        vnc_thread.start()
        Logger.success("Proxy VNC iniciado exitosamente")

        # 6. Esperar finalización
        while not stop_event.is_set():
            time.sleep(0.1)
            
        # 7. Cleanup
        try:
            Logger.debug("Cerrando conexión VNC")
            vnc_socket.close()
        except Exception as e:
            Logger.error(f"Error cerrando socket VNC: {str(e)}")
            
    except Exception as e:
        Logger.error(f"Error en proxy VNC: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        try:
            ws.send(f"Error interno: {str(e)}")
            ws.close(1011, "Error interno")
        except:
            pass
    finally:
        Logger.info(f"Finalizando proxy VNC para VM ID-{vm_id}")


# ===================== SKETCH =====================
@app.route('/create-sketch', methods=['POST'])
def create_sketch():
    """
    Crea un nuevo sketch en la base de datos.

    Request body:
    {
        "name": str,
        "description": str,
        "user_id": int,
        "topology_info": {
            "vms": [...],
            "links": [...],
            "interfaces": [...]
        }
    }

    Returns:
        Response: Mensaje de éxito/error y detalles
            200: Sketch creado exitosamente
            400: Error en request o recursos inválidos
            500: Error interno
    """
    try:
        Logger.major_section("API: CREATE SKETCH")
        request_data = request.get_json()
        Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
        
        # 1. Validar campos requeridos
        required_fields = ['name', 'user_id', 'topology_info']
        for field in required_fields:
            if field not in request_data:
                Logger.error(f"Campo requerido faltante: {field}")
                return jsonify({
                    "status": "error", 
                    "message": "Faltan campos requeridos",
                    "details": f"El campo '{field}' es requerido"
                }), 400

        # 2. Validar recursos (flavors e images)
        Logger.debug("Validando recursos...")
        topology = request_data['topology_info']
        user_id = request_data['user_id']
        
        # Obtener IDs únicos de flavors e images
        flavor_ids = {str(vm['flavor_id']) for vm in topology.get('vms', [])}
        image_ids = {str(vm['image_id']) for vm in topology.get('vms', [])}

        # Verificar flavors - públicos o del usuario
        Logger.debug(f"Verificando flavors: {flavor_ids}")
        valid_flavors = db.execute_query(
            """SELECT id FROM flavor 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(flavor_ids) if flavor_ids else 'NULL', user_id)
        )
        valid_flavor_ids = {str(f['id']) for f in valid_flavors}
        
        invalid_flavors = flavor_ids - valid_flavor_ids
        if invalid_flavors:
            Logger.error(f"Flavors inválidos o no accesibles: {invalid_flavors}")
            return jsonify({
                "status": "error",
                "message": "Algunos flavors no son válidos o no tienes acceso a ellos",
                "details": f"Los siguientes flavors no existen, no están activos o no tienes permiso: {list(invalid_flavors)}"
            }), 400

        # Verificar images - públicas o del usuario  
        Logger.debug(f"Verificando images: {image_ids}")
        valid_images = db.execute_query(
            """SELECT id FROM image 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(image_ids) if image_ids else 'NULL', user_id)
        )
        valid_image_ids = {str(i['id']) for i in valid_images}
        
        invalid_images = image_ids - valid_image_ids
        if invalid_images:
            Logger.error(f"Images inválidas o no accesibles: {invalid_images}")
            return jsonify({
                "status": "error",
                "message": "Algunas imágenes no son válidas o no tienes acceso a ellas",
                "details": f"Las siguientes imágenes no existen, no están activas o no tienes permiso: {list(invalid_images)}"
            }), 400
        # 3. Preparar datos para inserción
        structure = {
            "name": request_data['name'],
            "description": request_data.get('description', ''),
            "topology_info": request_data['topology_info']
        }

        # 4. Insertar en base de datos
        Logger.debug("Insertando sketch en base de datos")
        query = """
            INSERT INTO sketch (user, structure, created_at, updated_at)
            VALUES (%s, %s, NOW(), NOW())
        """
        
        db.execute_transaction([
            (query, (request_data['user_id'], json.dumps(structure)))
        ])

        Logger.success(f"Sketch creado exitosamente para usuario {request_data['user_id']}")
        return jsonify({
            "status": "success",
            "message": "Sketch creado exitosamente",
            "content": {
                "name": request_data['name'],
                "user_id": request_data['user_id'],
                "resources": {
                    "valid_flavors": list(valid_flavor_ids),
                    "valid_images": list(valid_image_ids)
                }
            }
        }), 200

    except Exception as e:
        Logger.error(f"Error creando sketch: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al crear el sketch",
            "details": str(e)
        }), 500

@app.route('/sketch/<int:sketch_id>', methods=['GET'])
def get_sketch(sketch_id):
    """
    Obtiene la información de un sketch específico.

    Args:
        sketch_id (int): ID del sketch a consultar

    Returns:
        Response: Información del sketch
            200: Sketch obtenido exitosamente
            404: Sketch no encontrado
            500: Error interno
    """
    try:
        Logger.major_section(f"API: GET SKETCH ID-{sketch_id}")

        # Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # Consultar sketch con validación de usuario
        Logger.debug(f"Consultando sketch ID-{sketch_id} para usuario {user_id}")
        query = """
            SELECT s.*, u.username 
            FROM sketch s
            JOIN user u ON s.user = u.id
            WHERE s.id = %s AND s.user = %s
        """
        result = db.execute_query(query, (sketch_id, user_id))

        if not result:
            # Verificar si el sketch existe
            exists = db.execute_query(
                "SELECT id FROM sketch WHERE id = %s", 
                (sketch_id,)
            )
            
            if exists:
                Logger.warning(f"Usuario {user_id} no autorizado para acceder al sketch {sketch_id}")
                return jsonify({
                    "status": "error",
                    "message": "No autorizado",
                    "details": "No tienes permiso para acceder a este sketch"
                }), 403
            else:
                Logger.warning(f"Sketch ID-{sketch_id} no encontrado")
                return jsonify({
                    "status": "error",
                    "message": "Sketch no encontrado",
                    "details": f"No existe un sketch con ID {sketch_id}"
                }), 404

        sketch = result[0]
        structure = json.loads(sketch['structure'])

        # Formatear respuesta
        response_data = {
            "id": sketch['id'],
            "name": structure['name'],
            "description": structure['description'],
            "topology_info": structure['topology_info'],
            "user": {
                "id": sketch['user'],
                "username": sketch['username']
            },
            "created_at": sketch['created_at'].isoformat(),
            "updated_at": sketch['updated_at'].isoformat() if sketch['updated_at'] else None
        }

        Logger.success(f"Sketch ID-{sketch_id} obtenido exitosamente")
        return jsonify({
            "status": "success",
            "message": "Sketch obtenido exitosamente",
            "content": response_data
        }), 200

    except Exception as e:
        Logger.error(f"Error obteniendo sketch: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al obtener el sketch",
            "details": str(e)
        }), 500

@app.route('/sketch/<int:sketch_id>', methods=['DELETE'])
def delete_sketch(sketch_id):
    """
    Elimina un sketch específico.

    Args:
        sketch_id (int): ID del sketch a eliminar

    Returns:
        Response: Mensaje de éxito/error
            200: Sketch eliminado exitosamente
            403: Usuario no autorizado
            404: Sketch no encontrado
            500: Error interno
    """
    try:
        Logger.major_section(f"API: DELETE SKETCH ID-{sketch_id}")

        # Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # Verificar si el sketch existe y pertenece al usuario
        Logger.debug(f"Verificando sketch ID-{sketch_id} para usuario {user_id}")
        check_query = """
            SELECT s.*, u.username 
            FROM sketch s
            JOIN user u ON s.user = u.id
            WHERE s.id = %s
        """
        sketch = db.execute_query(check_query, (sketch_id,))

        if not sketch:
            Logger.warning(f"Sketch ID-{sketch_id} no encontrado")
            return jsonify({
                "status": "error",
                "message": "Sketch no encontrado",
                "details": f"No existe un sketch con ID {sketch_id}"
            }), 404

        # Verificar que el sketch pertenezca al usuario
        if str(sketch[0]['user']) != str(user_id):
            Logger.warning(f"Usuario {user_id} no autorizado para eliminar sketch {sketch_id}")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "No tienes permiso para eliminar este sketch"
            }), 403

        # Eliminar sketch
        Logger.debug("Eliminando sketch de la base de datos")
        delete_query = "DELETE FROM sketch WHERE id = %s"
        db.execute_transaction([(delete_query, (sketch_id,))])

        Logger.success(f"Sketch ID-{sketch_id} eliminado exitosamente")
        return jsonify({
            "status": "success",
            "message": "Sketch eliminado exitosamente",
            "content": {
                "id": sketch_id,
                "name": json.loads(sketch[0]['structure'])['name']
            }
        }), 200

    except Exception as e:
        Logger.error(f"Error eliminando sketch: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al eliminar el sketch",
            "details": str(e)
        }), 500

@app.route('/sketch/<int:sketch_id>', methods=['PUT'])
def update_sketch(sketch_id):
    """
    Actualiza un sketch existente.
    Valida que el sketch pertenezca al usuario y que tenga acceso a los recursos.

    Args:
        sketch_id (int): ID del sketch a actualizar
        

    Request body:
    {
        "name": str,
        "description": str,
        "topology_info": {
            "vms": [...],
            "links": [...],
            "interfaces": [...]
        }
    }

    Returns:
        Response: Mensaje de éxito/error
            200: Sketch actualizado exitosamente
            400: Error en request o recursos inválidos
            403: Usuario no autorizado
            404: Sketch no encontrado
            500: Error interno
    """
    try:
        Logger.major_section(f"API: UPDATE SKETCH ID-{sketch_id}")
        request_data = request.get_json()
        Logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

        # 1. Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # 2. Verificar si el sketch existe y pertenece al usuario
        Logger.debug(f"Verificando permisos para usuario {user_id}")
        check_query = """
            SELECT s.id, s.user 
            FROM sketch s
            WHERE s.id = %s"""
        sketch = db.execute_query(check_query, (sketch_id,))

        if not sketch:
            Logger.warning(f"Sketch ID-{sketch_id} no encontrado")
            return jsonify({
                "status": "error",
                "message": "Sketch no encontrado",
                "details": f"No existe un sketch con ID {sketch_id}"
            }), 404

        # Verificar propiedad del sketch
        if str(sketch[0]['user']) != str(user_id):
            Logger.warning(f"Usuario {user_id} no autorizado para editar sketch {sketch_id}")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "No tienes permiso para editar este sketch"
            }), 403

        # 3. Validar recursos (flavors e images)
        Logger.debug("Validando recursos...")
        topology = request_data.get('topology_info', {})
        if not topology:
            Logger.error("No se proporcionó topology_info")
            return jsonify({
                "status": "error",
                "message": "Falta información de topología",
                "details": "El campo topology_info es requerido"
            }), 400
        
        # Obtener IDs únicos
        flavor_ids = {str(vm['flavor_id']) for vm in topology.get('vms', [])}
        image_ids = {str(vm['image_id']) for vm in topology.get('vms', [])}

        if not flavor_ids or not image_ids:
            Logger.error("No se encontraron IDs de recursos")
            return jsonify({
                "status": "error",
                "message": "Topología inválida",
                "details": "Cada VM debe especificar flavor_id e image_id"
            }), 400

        # Verificar flavors - públicos o del usuario
        Logger.debug(f"Verificando flavors: {flavor_ids}")
        valid_flavors = db.execute_query(
            """SELECT id FROM flavor 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(flavor_ids), user_id)
        )
        valid_flavor_ids = {str(f['id']) for f in valid_flavors}
        
        invalid_flavors = flavor_ids - valid_flavor_ids
        if invalid_flavors:
            Logger.error(f"Flavors inválidos o no accesibles: {invalid_flavors}")
            return jsonify({
                "status": "error",
                "message": "Algunos flavors no son válidos o no tienes acceso a ellos",
                "details": f"Los siguientes flavors no existen, no están activos o no tienes permiso: {list(invalid_flavors)}"
            }), 400

        # Verificar images - públicas o del usuario
        Logger.debug(f"Verificando images: {image_ids}")
        valid_images = db.execute_query(
            """SELECT id FROM image 
               WHERE id IN (%s) 
               AND state = 'active'
               AND (user IS NULL OR user = %s)""" % 
            (','.join(image_ids), user_id)
        )
        valid_image_ids = {str(i['id']) for i in valid_images}
        
        invalid_images = image_ids - valid_image_ids
        if invalid_images:
            Logger.error(f"Images inválidas o no accesibles: {invalid_images}")
            return jsonify({
                "status": "error",
                "message": "Algunas imágenes no son válidas o no tienes acceso a ellas", 
                "details": f"Las siguientes imágenes no existen, no están activas o no tienes permiso: {list(invalid_images)}"
            }), 400

        # 4. Preparar datos para actualización
        structure = {
            "name": request_data.get('name', ''),
            "description": request_data.get('description', ''),
            "topology_info": topology
        }

        # Validar nombre
        if not structure['name']:
            Logger.error("Nombre de sketch no proporcionado")
            return jsonify({
                "status": "error",
                "message": "Falta el nombre del sketch",
                "details": "El campo name es requerido"
            }), 400

        # 5. Actualizar en base de datos
        Logger.debug("Actualizando sketch en base de datos")
        update_query = """
            UPDATE sketch 
            SET structure = %s, updated_at = NOW()
            WHERE id = %s AND user = %s
        """
        
        db.execute_transaction([
            (update_query, (json.dumps(structure), sketch_id, user_id))
        ])

        Logger.success(f"Sketch ID-{sketch_id} actualizado exitosamente")
        return jsonify({
            "status": "success",
            "message": "Sketch actualizado exitosamente",
            "content": {
                "id": sketch_id,
                "name": structure['name'],
                "user_id": user_id,
                "resources": {
                    "valid_flavors": list(valid_flavor_ids),
                    "valid_images": list(valid_image_ids)
                }
            }
        }), 200

    except Exception as e:
        Logger.error(f"Error actualizando sketch: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al actualizar el sketch",
            "details": str(e)
        }), 500

@app.route('/list-sketches', methods=['GET'])
def list_sketches():
    """
    Obtiene todos los sketches de un usuario específico.
    
    Query params:
        

    Returns:
        Response: Lista de sketches del usuario
            200: Sketches obtenidos exitosamente
            400: user_id no proporcionado
            500: Error interno
    """
    try:
        Logger.major_section("API: LIST USER SKETCHES")
        
        # Validar user_id
        auth_token = request.headers.get('Authorization')
        if not auth_token:
            Logger.error("No se proporcionó token de autorización")
            return jsonify({
                "status": "error",
                "message": "No autorizado",
                "details": "Se requiere token de autorización"
            }), 401
            
        # Obtener username y rol del token
        username = jwt_manager.get_username_from_token(auth_token)
        if not username:
            Logger.error("Token inválido o expirado")
            return jsonify({
                "status": "error",
                "message": "Token inválido o expirado",
                "details": "El token de autorización no es válido o ha expirado"
            }), 401
            
        # Obtener rol del token
        role = jwt_manager.get_role_from_token(auth_token)
        Logger.debug(f"Rol del usuario: {role}")
            
        # Obtener user_id desde username
        token_user_id = jwt_manager.get_user_id_from_username(username, db)
        if not token_user_id:
            Logger.error(f"Usuario no encontrado: {username}")
            return jsonify({
                "status": "error", 
                "message": "Usuario no encontrado",
                "details": f"No se encontró el usuario con username: {username}"
            }), 404

        # Obtener user_id del query param si existe
        query_user_id = request.args.get('user_id')
        if query_user_id:
            query_user_id = int(query_user_id)
            
            # Si el user_id del query es diferente al del token, verificar si es Admin
            if query_user_id != token_user_id:
                if role != 'Admin':
                    Logger.warning(f"Usuario {token_user_id} intentó acceder a slices del usuario {query_user_id}")
                    return jsonify({
                        "status": "error",
                        "message": "No autorizado",
                        "details": "Solo los administradores pueden ver slices de otros usuarios"
                    }), 403
                Logger.debug(f"Admin {token_user_id} accediendo a slices del usuario {query_user_id}")
                user_id = query_user_id
            else:
                user_id = token_user_id
        else:
            user_id = token_user_id

        # Consultar sketches del usuario
        Logger.debug(f"Consultando sketches del usuario {user_id}")
        query = """
            SELECT s.*, u.username 
            FROM sketch s
            JOIN user u ON s.user = u.id
            WHERE s.user = %s
            ORDER BY s.created_at DESC
        """
        sketches = db.execute_query(query, (user_id,))
        
        # Formatear respuesta
        formatted_sketches = []
        for sketch in sketches:
            structure = json.loads(sketch['structure'])
            formatted_sketches.append({
                "id": sketch['id'],
                "name": structure['name'],
                "description": structure.get('description', ''),
                "vm_count": len(structure['topology_info']['vms']),
                "created_at": sketch['created_at'].isoformat(),
                "updated_at": sketch['updated_at'].isoformat() if sketch['updated_at'] else None,
                "user": {
                    "id": sketch['user'],
                    "username": sketch['username']
                }
            })

        Logger.success(f"Sketches obtenidos exitosamente para usuario {user_id}")
        return jsonify({
            "status": "success",
            "message": f"Se encontraron {len(formatted_sketches)} sketches",
            "content": formatted_sketches
        }), 200

    except Exception as e:
        Logger.error(f"Error listando sketches: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": "Error interno al listar sketches",
            "details": str(e)
        }), 500

# ===================== SERVER =====================
if __name__ == '__main__':
    try:
        Logger.major_section("INICIANDO SLICE MANAGER")
        
        # Inicializar componentes
        Logger.info("Inicializando componentes del sistema...")
        
        # Base de datos
        Logger.debug("Conectando a base de datos...")
        db = DatabaseManager()
        Logger.success("Conexión a base de datos establecida")
        
        # Gestor de tokens VNC
        Logger.debug("Inicializando gestor de tokens VNC...")
        vnc_token_manager = VNCTokenManager()
        Logger.success("Gestor de tokens VNC inicializado")

        # Gestor de tokens JWT del Usuario
        Logger.debug("Inicializando gestor de tokens JWT...")
        jwt_manager = JWTManager()
        Logger.success("Gestor de tokens JWT inicializado")
        
        # Configuración del servidor
        host = '0.0.0.0'
        port = 5001
        debug = False

        Logger.section("INICIANDO SERVIDOR WEB")
        
        Logger.info(f"Iniciando servidor en {host}:{port}")
        Logger.info("Presione Ctrl+C para detener el servidor")
        Logger.success("Slice Manager listo para recibir conexiones")

        # Iniciar servidor Flask
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True  # Habilitar múltiples threads
        )
        
    except Exception as e:
        Logger.error(f"Error iniciando el servidor: {str(e)}")
        Logger.debug(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)