#!/usr/bin/env python3
"""
Script para inicializar usuarios del sistema CRM
"""

import sqlite3
import uuid
import hashlib
import secrets
from datetime import date

DB_PATH = "muyu_crm.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(password: str, salt: str = None) -> tuple:
    """Hash password using SHA-256 with salt"""
    if salt is None:
        salt = secrets.token_hex(32)
    
    # Combine password and salt
    salted_password = password + salt
    # Hash using SHA-256
    hashed = hashlib.sha256(salted_password.encode()).hexdigest()
    
    return hashed, salt

def create_user(username: str, email: str, password: str, role: str, full_name: str = "") -> tuple:
    """Create new user"""
    try:
        conn = get_conn()
        c = conn.cursor()
        
        # Check if username or email already exists
        c.execute("SELECT id FROM users WHERE username = ? OR email = ?", (username, email))
        if c.fetchone():
            return False, f"Usuario {username} o email {email} ya existe"
        
        # Hash password
        password_hash, salt = hash_password(password)
        
        # Create user
        user_id = str(uuid.uuid4())
        c.execute('''
        INSERT INTO users (id, username, email, password_hash, salt, role, full_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, username, email, password_hash, salt, role, full_name, date.today()))
        
        conn.commit()
        conn.close()
        
        return True, f"Usuario {username} creado exitosamente"
        
    except Exception as e:
        return False, f"Error al crear usuario {username}: {str(e)}"

def init_users():
    """Initialize default users for the CRM system"""
    print("🚀 Inicializando usuarios del CRM Muyu")
    print("=" * 50)
    
    # Create users table if it doesn't exist
    conn = get_conn()
    c = conn.cursor()
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
    
    # Define users to create
    users_to_create = [
        {
            "username": "admin",
            "email": "admin@muyu.com",
            "password": "admin123",
            "role": "admin",
            "full_name": "Administrador CRM"
        },
        {
            "username": "ventas1",
            "email": "ventas1@muyu.com",
            "password": "ventas123",
            "role": "sales",
            "full_name": "Juan Pérez - Ventas"
        },
        {
            "username": "ventas2",
            "email": "ventas2@muyu.com",
            "password": "ventas123",
            "role": "sales",
            "full_name": "María González - Ventas"
        },
        {
            "username": "soporte1",
            "email": "soporte1@muyu.com",
            "password": "soporte123",
            "role": "support",
            "full_name": "Carlos Rodríguez - Soporte"
        },
        {
            "username": "soporte2",
            "email": "soporte2@muyu.com",
            "password": "soporte123",
            "role": "support",
            "full_name": "Ana Martínez - Soporte"
        }
    ]
    
    # Create users
    created_count = 0
    existing_count = 0
    
    for user_info in users_to_create:
        success, message = create_user(
            username=user_info["username"],
            email=user_info["email"],
            password=user_info["password"],
            role=user_info["role"],
            full_name=user_info["full_name"]
        )
        
        if success:
            print(f"✅ {message}")
            created_count += 1
        else:
            print(f"⚠️  {message}")
            existing_count += 1
    
    print("\n" + "=" * 50)
    print(f"🎉 Proceso completado!")
    print(f"✅ Usuarios creados: {created_count}")
    print(f"⚠️  Usuarios ya existentes: {existing_count}")
    
    print("\n📋 Credenciales de acceso:")
    print("-" * 30)
    for user_info in users_to_create:
        role_emoji = {"admin": "👑", "sales": "💼", "support": "🎧"}
        emoji = role_emoji.get(user_info["role"], "👤")
        print(f"{emoji} {user_info['role'].upper()}")
        print(f"   Usuario: {user_info['username']}")
        print(f"   Contraseña: {user_info['password']}")
        print(f"   Email: {user_info['email']}")
        print()
    
    print("🚀 Ahora puedes ejecutar la aplicación:")
    print("   streamlit run app1.py")
    print()
    print("💡 Inicia sesión con cualquiera de las credenciales mostradas arriba")

if __name__ == "__main__":
    init_users()