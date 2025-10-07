import streamlit as st
import sqlite3
import uuid
from datetime import date
from auth.jwt_manager import JWTManager

DB_PATH = "muyu_crm.db"

def get_conn():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def init_auth_db():
    """Initialize authentication tables"""
    conn = get_conn()
    c = conn.cursor()
    
    # Users table for authentication
    c.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'sales',
        full_name TEXT,
        created_at DATE DEFAULT CURRENT_DATE,
        last_login DATE,
        is_active INTEGER DEFAULT 1
    )
    ''')
    
    conn.commit()
    conn.close()

def create_user(username: str, email: str, password: str, role: str, full_name: str = "") -> tuple:
    """Create new user"""
    try:
        conn = get_conn()
        c = conn.cursor()
        
        # Check if username or email already exists
        c.execute("SELECT id FROM users WHERE username = ? OR email = ?", (username, email))
        if c.fetchone():
            return False, "Usuario o email ya existe"
        
        # Hash password
        password_hash, salt = JWTManager.hash_password(password)
        
        # Create user
        user_id = str(uuid.uuid4())
        c.execute('''
        INSERT INTO users (id, username, email, password_hash, salt, role, full_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, username, email, password_hash, salt, role, full_name, date.today()))
        
        conn.commit()
        conn.close()
        
        return True, "Usuario creado exitosamente"
        
    except Exception as e:
        return False, f"Error al crear usuario: {str(e)}"

def authenticate_user(username: str, password: str) -> tuple:
    """Authenticate user credentials"""
    try:
        conn = get_conn()
        c = conn.cursor()
        
        # Get user data
        c.execute('''
        SELECT id, username, email, password_hash, salt, role, full_name, is_active
        FROM users 
        WHERE username = ? AND is_active = 1
        ''', (username,))
        
        user = c.fetchone()
        if not user:
            return False, None, "Usuario no encontrado o inactivo"
        
        # Verify password
        if not JWTManager.verify_password(password, user['password_hash'], user['salt']):
            return False, None, "Contraseña incorrecta"
        
        # Update last login
        c.execute("UPDATE users SET last_login = ? WHERE id = ?", (date.today(), user['id']))
        conn.commit()
        
        # Create user data dict
        user_data = {
            "id": user['id'],
            "username": user['username'],
            "email": user['email'],
            "role": user['role'],
            "full_name": user['full_name']
        }
        
        conn.close()
        return True, user_data, "Login exitoso"
        
    except Exception as e:
        return False, None, f"Error en autenticación: {str(e)}"

def get_users_list():
    """Get list of all users (admin only)"""
    conn = get_conn()
    c = conn.cursor()
    
    c.execute('''
    SELECT id, username, email, role, full_name, created_at, last_login, is_active
    FROM users
    ORDER BY created_at DESC
    ''')
    
    users = c.fetchall()
    conn.close()
    
    return [dict(user) for user in users]

def update_user_status(user_id: str, is_active: bool):
    """Update user active status"""
    conn = get_conn()
    c = conn.cursor()
    
    c.execute("UPDATE users SET is_active = ? WHERE id = ?", (int(is_active), user_id))
    conn.commit()
    conn.close()

def create_admin_user():
    """Create default admin user if none exists"""
    conn = get_conn()
    c = conn.cursor()
    
    # Check if admin user exists
    c.execute("SELECT id FROM users WHERE role = 'admin'")
    if c.fetchone():
        conn.close()
        return False, "Usuario admin ya existe"
    
    # Create admin user
    success, message = create_user(
        username="admin",
        email="admin@muyu.com",
        password="admin123",  # Change this in production!
        role="admin",
        full_name="Administrador"
    )
    
    conn.close()
    return success, message

# Initialize auth database
init_auth_db()

def show_login_page():
    """Display login page"""
    st.markdown("## 🔐 Iniciar Sesión - Muyu CRM")
    
    # Create admin user button (only show if no admin exists)
    if st.sidebar.button("Crear Usuario Admin (Solo primera vez)"):
        success, message = create_admin_user()
        if success:
            st.sidebar.success(message)
            st.sidebar.info("Usuario: admin, Contraseña: admin123")
        else:
            st.sidebar.warning(message)
    
    with st.form("login_form"):
        st.markdown("### Ingresa tus credenciales")
        
        username = st.text_input("Usuario", placeholder="Ingresa tu usuario")
        password = st.text_input("Contraseña", type="password", placeholder="Ingresa tu contraseña")
        
        col1, col2 = st.columns(2)
        
        with col1:
            login_button = st.form_submit_button("🚀 Iniciar Sesión", use_container_width=True)
        
        with col2:
            register_button = st.form_submit_button("📝 Registrarse", use_container_width=True)
    
    if login_button:
        if username and password:
            success, user_data, message = authenticate_user(username, password)
            
            if success:
                # Create JWT token
                token = JWTManager.create_token(user_data)
                JWTManager.login(token)
                
                st.success(f"¡Bienvenido {user_data['full_name'] or user_data['username']}!")
                st.info(f"Rol: {user_data['role'].title()}")
                st.rerun()
            else:
                st.error(message)
        else:
            st.error("Por favor, completa todos los campos")
    
    if register_button:
        st.session_state.show_register = True
        st.rerun()

def show_register_page():
    """Display registration page"""
    st.markdown("## 📝 Registro de Usuario - Muyu CRM")
    
    with st.form("register_form"):
        st.markdown("### Crear nueva cuenta")
        
        col1, col2 = st.columns(2)
        
        with col1:
            username = st.text_input("Usuario*", placeholder="Nombre de usuario único")
            email = st.text_input("Email*", placeholder="correo@empresa.com")
            full_name = st.text_input("Nombre Completo", placeholder="Nombre y apellidos")
        
        with col2:
            password = st.text_input("Contraseña*", type="password", placeholder="Mínimo 6 caracteres")
            confirm_password = st.text_input("Confirmar Contraseña*", type="password", placeholder="Confirma tu contraseña")
            role = st.selectbox("Rol*", ["sales", "support"], 
                              help="Admin solo puede ser asignado por otro admin")
        
        col1, col2 = st.columns(2)
        
        with col1:
            register_button = st.form_submit_button("✅ Crear Cuenta", use_container_width=True)
        
        with col2:
            back_button = st.form_submit_button("⬅️ Volver al Login", use_container_width=True)
    
    if register_button:
        # Validation
        if not all([username, email, password, confirm_password]):
            st.error("Por favor, completa todos los campos obligatorios (*)")
        elif len(password) < 6:
            st.error("La contraseña debe tener al menos 6 caracteres")
        elif password != confirm_password:
            st.error("Las contraseñas no coinciden")
        elif "@" not in email:
            st.error("Por favor, ingresa un email válido")
        else:
            success, message = create_user(username, email, password, role, full_name)
            
            if success:
                st.success(message)
                st.info("Ahora puedes iniciar sesión con tu nueva cuenta")
                st.session_state.show_register = False
                st.rerun()
            else:
                st.error(message)
    
    if back_button:
        st.session_state.show_register = False
        st.rerun()

def show_auth_interface():
    """Main authentication interface"""
    if st.session_state.get("show_register", False):
        show_register_page()
    else:
        show_login_page()