import streamlit as st
from auth.jwt_manager import JWTManager
from auth.login import show_auth_interface
from auth.dashboards import show_dashboard

# Configure Streamlit page
st.set_page_config(
    page_title="Muyu CRM",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #1f4e79, #2e86de);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    
    .auth-container {
        max-width: 500px;
        margin: 0 auto;
        padding: 2rem;
        background: #f8f9fa;
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .role-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 600;
    }
    
    .role-admin {
        background-color: #dc3545;
        color: white;
    }
    
    .role-sales {
        background-color: #28a745;
        color: white;
    }
    
    .role-support {
        background-color: #007bff;
        color: white;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Main application logic"""
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🚀 Muyu CRM</h1>
        <p>Sistema de Gestión de Relaciones con Clientes</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check authentication status
    if not JWTManager.is_logged_in():
        # Show authentication interface
        st.markdown('<div class="auth-container">', unsafe_allow_html=True)
        show_auth_interface()
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Add some information about the system
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 👑 Administrador
            - Acceso completo al sistema
            - Gestión de usuarios
            - Reportes avanzados
            - Configuración del sistema
            """)
        
        with col2:
            st.markdown("""
            ### 💼 Ventas
            - Gestión del CRM
            - Seguimiento de clientes
            - Tareas de seguimiento
            - Métricas de ventas
            """)
        
        with col3:
            st.markdown("""
            ### 🎧 Soporte
            - Gestión de tickets
            - Chat de soporte
            - Base de conocimiento
            - Atención al cliente
            """)
    
    else:
        # User is logged in, show appropriate dashboard
        show_dashboard()

if __name__ == "__main__":
    main()