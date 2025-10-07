import jwt
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import streamlit as st

# Secret key for JWT (in production, use environment variable)
JWT_SECRET = "your-secret-key-change-this-in-production"
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

class JWTManager:
    """Handles JWT token creation, validation and user session management"""
    
    @staticmethod
    def hash_password(password: str, salt: str = None) -> tuple:
        """Hash password using SHA-256 with salt"""
        if salt is None:
            salt = secrets.token_hex(32)
        
        # Combine password and salt
        salted_password = password + salt
        # Hash using SHA-256
        hashed = hashlib.sha256(salted_password.encode()).hexdigest()
        
        return hashed, salt
    
    @staticmethod
    def verify_password(password: str, hashed_password: str, salt: str) -> bool:
        """Verify password against hash"""
        test_hash, _ = JWTManager.hash_password(password, salt)
        return test_hash == hashed_password
    
    @staticmethod
    def create_token(user_data: Dict[str, Any]) -> str:
        """Create JWT token for user"""
        payload = {
            "user_id": user_data["id"],
            "username": user_data["username"],
            "email": user_data["email"],
            "role": user_data["role"],
            "full_name": user_data.get("full_name", ""),
            "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
            "iat": datetime.utcnow()
        }
        
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return token
    
    @staticmethod
    def decode_token(token: str) -> Optional[Dict[str, Any]]:
        """Decode and validate JWT token"""
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            st.error("Sesión expirada. Por favor, inicia sesión nuevamente.")
            return None
        except jwt.InvalidTokenError:
            st.error("Token inválido. Por favor, inicia sesión nuevamente.")
            return None
    
    @staticmethod
    def is_logged_in() -> bool:
        """Check if user is logged in"""
        return "jwt_token" in st.session_state and st.session_state.jwt_token is not None
    
    @staticmethod
    def get_current_user() -> Optional[Dict[str, Any]]:
        """Get current user from session"""
        if not JWTManager.is_logged_in():
            return None
        
        token = st.session_state.jwt_token
        user_data = JWTManager.decode_token(token)
        
        if user_data is None:
            # Token invalid, clear session
            JWTManager.logout()
            return None
        
        return user_data
    
    @staticmethod
    def login(token: str) -> None:
        """Login user by storing JWT token in session"""
        st.session_state.jwt_token = token
        st.session_state.logged_in = True
    
    @staticmethod
    def logout() -> None:
        """Logout user by clearing session"""
        if "jwt_token" in st.session_state:
            del st.session_state.jwt_token
        if "logged_in" in st.session_state:
            del st.session_state.logged_in
        st.rerun()
    
    @staticmethod
    def require_role(required_role: str) -> bool:
        """Check if current user has required role"""
        user = JWTManager.get_current_user()
        if not user:
            return False
        
        user_role = user.get("role", "")
        
        # Admin has access to everything
        if user_role == "admin":
            return True
        
        # Check specific role
        return user_role == required_role
    
    @staticmethod
    def get_user_role() -> str:
        """Get current user's role"""
        user = JWTManager.get_current_user()
        return user.get("role", "") if user else ""