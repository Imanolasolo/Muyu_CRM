import streamlit as st
from auth.jwt_manager import JWTManager
from auth.login import get_users_list, update_user_status

def show_admin_dashboard():
    """Admin dashboard with full access"""
    st.markdown("## 👑 Panel de Administración")
    
    user = JWTManager.get_current_user()
    st.markdown(f"**Bienvenido {user['full_name'] or user['username']}** - Rol: Administrador")
    
    # Admin tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🏢 CRM Completo", "👥 Gestión de Usuarios", "📊 Reportes", "⚙️ Configuración"])
    
    with tab1:
        st.markdown("### 🏢 Sistema CRM Completo")
        st.info("Acceso completo a todas las funcionalidades del CRM")
        
        # Import and show CRM module
        try:
            from modules.crm import crm_dashboard
            crm_dashboard()
        except ImportError:
            st.error("Módulo CRM no encontrado")
    
    with tab2:
        show_user_management()
    
    with tab3:
        st.markdown("### 📊 Reportes Avanzados")
        st.info("Reportes y analytics para administradores")
        # Add advanced reporting here
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Usuarios Activos", "5", "↗️ +2")
        
        with col2:
            st.metric("Instituciones", "150", "↗️ +10")
        
        with col3:
            st.metric("Tareas Pendientes", "23", "↘️ -5")
    
    with tab4:
        st.markdown("### ⚙️ Configuración del Sistema")
        st.info("Configuraciones avanzadas del sistema")
        
        if st.button("🔄 Reiniciar Cache"):
            st.cache_data.clear()
            st.success("Cache reiniciado")

def show_sales_dashboard():
    """Sales dashboard with CRM access"""
    st.markdown("## 💼 Panel de Ventas")
    
    user = JWTManager.get_current_user()
    st.markdown(f"**Bienvenido {user['full_name'] or user['username']}** - Rol: Ventas")
    
    # Sales tabs
    tab1, tab2, tab3 = st.tabs(["🏢 CRM", "📋 Mis Tareas", "📈 Mi Rendimiento"])
    
    with tab1:
        st.markdown("### 🏢 Sistema CRM")
        st.info("Gestión de clientes y oportunidades de venta")
        
        # Import and show CRM module
        try:
            from modules.crm import crm_dashboard
            crm_dashboard()
        except ImportError:
            st.error("Módulo CRM no encontrado")
    
    with tab2:
        st.markdown("### 📋 Mis Tareas")
        st.info("Seguimiento y gestión de tareas de ventas")
        # Add task management specific to sales
        
        # Sample tasks
        tasks = [
            {"cliente": "Universidad ABC", "tarea": "Seguimiento propuesta", "fecha": "2024-01-15", "prioridad": "Alta"},
            {"cliente": "Instituto XYZ", "tarea": "Llamada de cierre", "fecha": "2024-01-16", "prioridad": "Media"},
            {"cliente": "Colegio DEF", "tarea": "Enviar documentación", "fecha": "2024-01-17", "prioridad": "Baja"},
        ]
        
        for task in tasks:
            with st.expander(f"📅 {task['cliente']} - {task['tarea']}"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Fecha:** {task['fecha']}")
                with col2:
                    st.write(f"**Prioridad:** {task['prioridad']}")
                with col3:
                    if st.button(f"✅ Completar", key=f"task_{task['cliente']}"):
                        st.success("Tarea completada!")
    
    with tab3:
        st.markdown("### 📈 Mi Rendimiento")
        st.info("Métricas y estadísticas de ventas personales")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Clientes Asignados", "45", "↗️ +5")
        
        with col2:
            st.metric("Propuestas Enviadas", "12", "↗️ +3")
        
        with col3:
            st.metric("Contratos Cerrados", "8", "↗️ +2")

def show_support_dashboard():
    """Support dashboard with limited access"""
    st.markdown("## 🎧 Panel de Soporte")
    
    user = JWTManager.get_current_user()
    st.markdown(f"**Bienvenido {user['full_name'] or user['username']}** - Rol: Soporte")
    
    # Support tabs
    tab1, tab2, tab3 = st.tabs(["📞 Tickets", "💬 Chat Support", "📚 Base de Conocimiento"])
    
    with tab1:
        st.markdown("### 📞 Gestión de Tickets")
        st.info("Sistema de tickets de soporte al cliente")
        
        # Sample tickets
        tickets = [
            {"id": "TK001", "cliente": "Universidad ABC", "asunto": "Problema con acceso", "estado": "Abierto", "prioridad": "Alta"},
            {"id": "TK002", "cliente": "Instituto XYZ", "asunto": "Consulta sobre funcionalidad", "estado": "En progreso", "prioridad": "Media"},
            {"id": "TK003", "cliente": "Colegio DEF", "asunto": "Solicitud de capacitación", "estado": "Cerrado", "prioridad": "Baja"},
        ]
        
        for ticket in tickets:
            with st.expander(f"🎫 {ticket['id']} - {ticket['asunto']}"):
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.write(f"**Cliente:** {ticket['cliente']}")
                
                with col2:
                    st.write(f"**Estado:** {ticket['estado']}")
                
                with col3:
                    st.write(f"**Prioridad:** {ticket['prioridad']}")
                
                with col4:
                    if st.button(f"📝 Responder", key=f"ticket_{ticket['id']}"):
                        st.text_area("Respuesta:", key=f"response_{ticket['id']}")
    
    with tab2:
        st.markdown("### 💬 Chat de Soporte")
        st.info("Herramientas de chat y comunicación con clientes")
        
        # Chat interface placeholder
        st.text_area("Mensaje de soporte:", placeholder="Escribe tu respuesta aquí...")
        if st.button("📤 Enviar Mensaje"):
            st.success("Mensaje enviado al cliente")
    
    with tab3:
        st.markdown("### 📚 Base de Conocimiento")
        st.info("Documentación y recursos para resolver consultas")
        
        # Knowledge base categories
        categories = ["🔧 Problemas Técnicos", "❓ Preguntas Frecuentes", "📖 Manuales", "🎥 Tutoriales"]
        
        for category in categories:
            with st.expander(category):
                st.write("Contenido de ejemplo para esta categoría...")
                st.button(f"Ver más sobre {category.split(' ', 1)[1]}")

def show_user_management():
    """User management interface (admin only)"""
    st.markdown("### 👥 Gestión de Usuarios")
    
    # Get users list
    users = get_users_list()
    
    if not users:
        st.info("No hay usuarios registrados")
        return
    
    # Display users table
    st.markdown("#### Usuarios Registrados")
    
    for user in users:
        with st.expander(f"👤 {user['username']} - {user['full_name']} ({user['role']})"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write(f"**Email:** {user['email']}")
                st.write(f"**Rol:** {user['role']}")
                st.write(f"**Estado:** {'✅ Activo' if user['is_active'] else '❌ Inactivo'}")
            
            with col2:
                st.write(f"**Creado:** {user['created_at']}")
                st.write(f"**Último login:** {user['last_login'] or 'Nunca'}")
            
            with col3:
                # Toggle user status
                current_status = bool(user['is_active'])
                new_status = st.checkbox(
                    "Usuario Activo", 
                    value=current_status, 
                    key=f"user_status_{user['id']}"
                )
                
                if new_status != current_status:
                    update_user_status(user['id'], new_status)
                    st.success(f"Estado de {user['username']} actualizado")
                    st.rerun()

def show_dashboard():
    """Main dashboard router based on user role"""
    user = JWTManager.get_current_user()
    
    if not user:
        st.error("Error al obtener información del usuario")
        JWTManager.logout()
        return
    
    # Logout button in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**👤 Usuario:** {user['username']}")
    st.sidebar.markdown(f"**📧 Email:** {user['email']}")
    st.sidebar.markdown(f"**🎭 Rol:** {user['role'].title()}")
    
    if st.sidebar.button("🚪 Cerrar Sesión"):
        JWTManager.logout()
    
    # Route to appropriate dashboard based on role
    role = user['role']
    
    if role == "admin":
        show_admin_dashboard()
    elif role == "sales":
        show_sales_dashboard()
    elif role == "support":
        show_support_dashboard()
    else:
        st.error(f"Rol no reconocido: {role}")
        JWTManager.logout()