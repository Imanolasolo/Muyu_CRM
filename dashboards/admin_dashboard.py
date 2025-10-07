"""
Admin Dashboard - Panel completo de administración
Incluye todas las funcionalidades del CRM para administradores
"""

import streamlit as st
import sqlite3
from datetime import datetime, timedelta, date
import pandas as pd
import altair as alt
import uuid
import io
from pytz import timezone
import urllib.parse
import smtplib
import email.mime.text
import email.mime.multipart

# ----------------------
# Database utilities
# ----------------------
DB_PATH = "muyu_crm.db"

# ----------------------
# Email Configuration (Hardcoded)
# ----------------------
ADMIN_EMAIL = "jjusturi@gmail.com"  # Cambia por el email real del administrador
ADMIN_APP_PASSWORD = "qops yine aeup uxdf"  # Cambia por la contraseña de aplicación real

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

# ----------------------
# Helpers
# ----------------------

def now_date():
    return datetime.now().date()

def safe_date_value(date_value):
    """Convierte un valor de fecha de pandas/datetime a un valor seguro para st.date_input"""
    if date_value is None:
        return None
    
    # Verificar si es un pandas NaT
    try:
        if pd.isna(date_value):
            return None
    except (TypeError, ValueError):
        pass
    
    # Verificar si es un pandas Timestamp que es NaT
    if hasattr(date_value, 'isna') and date_value.isna():
        return None
        
    # Intentar convertir a date
    try:
        if hasattr(date_value, 'date'):
            result = date_value.date()
            # Verificar que el resultado no sea None
            if result is not None:
                return result
        elif isinstance(date_value, str):
            # Si es string, intentar parsear
            parsed = pd.to_datetime(date_value, errors='coerce')
            if not pd.isna(parsed):
                return parsed.date()
        else:
            return date_value
    except (ValueError, AttributeError, TypeError):
        pass
    
    return None

def safe_date_display(date_value):
    """Convierte un valor de fecha a string seguro para mostrar en UI"""
    if date_value is None:
        return 'N/A'
    
    try:
        if pd.isna(date_value):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    
    try:
        if isinstance(date_value, str):
            # Si es string, convertir a datetime primero
            dt = pd.to_datetime(date_value, errors='coerce')
            if not pd.isna(dt):
                return dt.date()
            else:
                return date_value  # Retornar el string original si no se puede parsear
        elif hasattr(date_value, 'date'):
            # Si ya es datetime, obtener solo la fecha
            return date_value.date()
        else:
            return str(date_value)
    except:
        return str(date_value) if date_value else 'N/A'

def save_institution(data: dict):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
    INSERT OR REPLACE INTO institutions (id,name,rector_name,rector_email,rector_phone,contraparte_name,contraparte_email,contraparte_phone,website,pais,ciudad,direccion,created_contact,last_interaction,num_teachers,num_students,avg_fee,initial_contact_medium,stage,substage,program_proposed,proposal_value,contract_start_date,contract_end_date,observations,assigned_commercial,no_interest_reason)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        data['id'], data['name'], data.get('rector_name'), data.get('rector_email'), data.get('rector_phone'),
        data.get('contraparte_name'), data.get('contraparte_email'), data.get('contraparte_phone'),
        data.get('website'), data.get('pais'), data.get('ciudad'), data.get('direccion'),
        data.get('created_contact'), data.get('last_interaction'),
        data.get('num_teachers'), data.get('num_students'), data.get('avg_fee'), data.get('initial_contact_medium'),
        data.get('stage'), data.get('substage'), data.get('program_proposed'), data.get('proposal_value'), 
        data.get('contract_start_date'), data.get('contract_end_date'), data.get('observations'),
        data.get('assigned_commercial'), data.get('no_interest_reason')
    ))
    conn.commit()
    conn.close()

def get_available_users():
    """Obtiene lista de usuarios activos para asignar como responsables"""
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT username, full_name, role FROM users WHERE is_active = 1 ORDER BY full_name, username')
    users = c.fetchall()
    conn.close()
    
    # Crear lista de opciones para el selectbox
    user_options = ["Sin asignar"]
    user_mapping = {"Sin asignar": ""}
    
    for user in users:
        username = user[0]
        full_name = user[1] if user[1] else username
        role = user[2]
        
        # Formato: "Nombre Completo (username) - Rol"
        display_name = f"{full_name} ({username}) - {role.title()}"
        user_options.append(display_name)
        user_mapping[display_name] = username
    
    return user_options, user_mapping

def fetch_institutions_df(columns=None, where_clause=None, limit=None):
    """Fetch institutions with optimized queries
    
    Args:
        columns: List of columns to select (default: all)
        where_clause: SQL WHERE clause for filtering
        limit: Maximum number of records to return
    """
    conn = get_conn()
    
    # Build optimized query
    if columns:
        column_str = ', '.join(columns)
    else:
        column_str = '*'
    
    query = f'SELECT {column_str} FROM institutions'
    
    if where_clause:
        query += f' WHERE {where_clause}'
    
    if limit:
        query += f' LIMIT {limit}'
    
    df = pd.read_sql_query(query, conn)
    
    # Convertir a datetime solo si las columnas están presentes
    date_columns = ['created_contact', 'last_interaction']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    conn.close()
    return df

def add_interaction(institution_id, medium, notes, date=None):
    conn = get_conn()
    c = conn.cursor()
    iid = str(uuid.uuid4())
    d = date or now_date()
    c.execute('INSERT INTO interactions (id,institution_id,date,medium,notes) VALUES (?,?,?,?,?)', (iid,institution_id,str(d),medium,notes))
    # update last_interaction in institutions
    c.execute('UPDATE institutions SET last_interaction = ? WHERE id = ?', (str(d), institution_id))
    conn.commit()
    conn.close()

# Función create_task movida más abajo para evitar duplicación

def get_institutions_metrics():
    """Get basic metrics without loading full dataset"""
    conn = get_conn()
    c = conn.cursor()
    
    # Count by stage
    c.execute('SELECT stage, COUNT(*) as count FROM institutions GROUP BY stage')
    stage_counts = dict(c.fetchall())
    
    # Total count
    c.execute('SELECT COUNT(*) as total FROM institutions')
    total = c.fetchone()[0]
    
    # Get unique values for filters without loading full data
    c.execute('SELECT DISTINCT pais FROM institutions WHERE pais IS NOT NULL ORDER BY pais')
    paises = [row[0] for row in c.fetchall()]
    
    c.execute('SELECT DISTINCT ciudad FROM institutions WHERE ciudad IS NOT NULL ORDER BY ciudad')
    ciudades = [row[0] for row in c.fetchall()]
    
    conn.close()
    
    return {
        'total': total,
        'stage_counts': stage_counts,
        'paises': paises,
        'ciudades': ciudades
    }

def get_users_metrics():
    """Get user metrics without loading full user data"""
    conn = get_conn()
    c = conn.cursor()
    
    c.execute('SELECT COUNT(*) as total FROM users')
    total = c.fetchone()[0]
    
    c.execute('SELECT COUNT(*) as active FROM users WHERE is_active = 1')
    active = c.fetchone()[0]
    
    c.execute('SELECT role, COUNT(*) as count FROM users GROUP BY role')
    role_counts = dict(c.fetchall())
    
    conn.close()
    
    return {
        'total': total,
        'active': active,
        'by_role': role_counts
    }

def get_sales_support_users():
    """Obtener usuarios con rol de sales o support para asignación de tareas"""
    conn = get_conn()
    c = conn.cursor()
    
    c.execute('''
        SELECT username, full_name, email, role 
        FROM users 
        WHERE role IN ('sales', 'support') AND is_active = 1
        ORDER BY full_name, username
    ''')
    
    users = c.fetchall()
    conn.close()
    
    # Formatear para selectbox: "Nombre Completo (username) - Rol"
    user_options = []
    user_data = {}
    
    for username, full_name, email, role in users:
        display_name = f"{full_name or username} ({username}) - {role.title()}"
        user_options.append(display_name)
        user_data[display_name] = {
            'username': username,
            'full_name': full_name,
            'email': email,
            'role': role
        }
    
    return user_options, user_data

def send_task_email(task_data, responsable_info):
    """Enviar tarea por email al responsable usando SMTP directo"""
    try:
        # Validar configuración
        if ADMIN_EMAIL == "tu_email@gmail.com" or ADMIN_APP_PASSWORD == "tu_contraseña_app":
            return False, "⚠️ Configura primero ADMIN_EMAIL y ADMIN_APP_PASSWORD en el código"
        
        # Configurar el mensaje
        msg = email.mime.multipart.MIMEMultipart()
        msg['From'] = ADMIN_EMAIL
        msg['To'] = responsable_info.get('email', '')
        msg['Subject'] = f"Nueva Tarea Asignada: {task_data['title']}"
        
        # Crear el cuerpo del email con formato HTML
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2E86AB;">🎯 Nueva Tarea Asignada - CRM Muyu</h2>
            
            <p>Hola <strong>{responsable_info.get('full_name', responsable_info.get('username', ''))}</strong>,</p>
            
            <p>Se te ha asignado una nueva tarea en el CRM de Muyu:</p>
            
            <div style="background-color: #f8f9fa; padding: 20px; border-left: 4px solid #2E86AB; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #2E86AB;">📋 DETALLES DE LA TAREA</h3>
                <ul style="list-style: none; padding: 0;">
                    <li><strong>📝 Título:</strong> {task_data['title']}</li>
                    <li><strong>🏢 Institución:</strong> {task_data['institucion']}</li>
                    <li><strong>📅 Fecha de vencimiento:</strong> {task_data['due_date'].strftime('%d/%m/%Y') if not pd.isna(task_data['due_date']) else 'No definida'}</li>
                    <li><strong>📊 Estado:</strong> {'✅ Completada' if task_data['done'] else '⏳ Pendiente'}</li>
                </ul>
            </div>
            
            <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                <h4 style="margin-top: 0; color: #856404;">📝 NOTAS:</h4>
                <p style="margin-bottom: 0;">{task_data['notes'] or 'Sin notas adicionales'}</p>
            </div>
            
            <p>Por favor, revisa esta tarea en el sistema CRM y toma las acciones necesarias.</p>
            
            <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
            
            <p style="color: #666; font-size: 14px;">
                Saludos,<br>
                <strong>Administrador CRM Muyu</strong><br>
                <em>Este es un mensaje automático del sistema CRM</em>
            </p>
        </body>
        </html>
        """
        
        # Adjuntar el cuerpo HTML
        msg.attach(email.mime.text.MIMEText(html_body, 'html', 'utf-8'))
        
        # Conectar y enviar
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(ADMIN_EMAIL, ADMIN_APP_PASSWORD)
        
        # Enviar el mensaje
        text = msg.as_string()
        server.sendmail(ADMIN_EMAIL, responsable_info.get('email', ''), text)
        server.quit()
        
        return True, "✅ Email enviado correctamente"
        
    except smtplib.SMTPAuthenticationError:
        return False, "❌ Error de autenticación. Verifica el email y contraseña de aplicación"
    except smtplib.SMTPException as e:
        return False, f"❌ Error SMTP: {str(e)}"
    except Exception as e:
        return False, f"❌ Error al enviar email: {str(e)}"

def send_task_whatsapp(task_data, responsable_info):
    """Crear mensaje de WhatsApp para enviar tarea al responsable"""
    try:
        # Extraer información del responsable de las notas
        notes_text = str(task_data['notes'])
        whatsapp_number = ""
        
        if "WhatsApp:" in notes_text:
            lines = notes_text.split('\n')
            for line in lines:
                if line.startswith('WhatsApp:'):
                    whatsapp_number = line.replace('WhatsApp:', '').strip()
                    break
        
        if not whatsapp_number:
            return False, "No se encontró número de WhatsApp del responsable"
        
        # Crear mensaje
        message = f"""
🎯 *Nueva Tarea Asignada - CRM Muyu*

Hola {responsable_info.get('full_name', responsable_info.get('username', ''))},

📋 *DETALLES DE LA TAREA:*
• *Título:* {task_data['title']}
• *Institución:* {task_data['institucion']}
• *Vencimiento:* {task_data['due_date'].strftime('%d/%m/%Y') if not pd.isna(task_data['due_date']) else 'No definida'}
• *Estado:* {'✅ Completada' if task_data['done'] else '⏳ Pendiente'}

📝 *NOTAS:*
{task_data['notes'] or 'Sin notas adicionales'}

Por favor revisa esta tarea en el CRM y toma las acciones necesarias.

_Mensaje enviado por Admin CRM Muyu_
        """.strip()
        
        # Crear URL de WhatsApp
        encoded_message = urllib.parse.quote(message)
        # Limpiar número (quitar espacios, guiones, etc.)
        clean_number = ''.join(filter(str.isdigit, whatsapp_number))
        whatsapp_url = f"https://wa.me/{clean_number}?text={encoded_message}"
        
        return True, whatsapp_url
        
    except Exception as e:
        return False, f"Error al generar WhatsApp: {str(e)}"

def extract_responsable_info_from_notes(notes):
    """Extraer información del responsable de las notas de la tarea"""
    if not notes:
        return {}
    
    info = {}
    lines = str(notes).split('\n')
    
    for line in lines:
        if line.startswith('Responsable:') and '(' in line and ')' in line:
            # Extraer nombre y username: "Responsable: Juan Pérez (jperez)"
            responsable_part = line.replace('Responsable:', '').strip()
            if '(' in responsable_part:
                full_name = responsable_part.split('(')[0].strip()
                username = responsable_part.split('(')[1].replace(')', '').strip()
                info['full_name'] = full_name
                info['username'] = username
        elif line.startswith('Email:'):
            info['email'] = line.replace('Email:', '').strip()
        elif line.startswith('Rol:'):
            info['role'] = line.replace('Rol:', '').strip()
        elif line.startswith('WhatsApp:'):
            info['whatsapp'] = line.replace('WhatsApp:', '').strip()
    
    return info

# ----------------------
# Admin Dashboard Main Function
# ----------------------

def show_admin_dashboard():
    """Mostrar dashboard completo de administrador optimizado"""
    
    
    
    # Información sobre optimización y botón para limpiar cache
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Acceso completo al sistema CRM - Funcionalidades de administrador**")
    with col2:
        if st.button("🧹 Limpiar Todo"):
            # Limpiar todos los estados de cache
            st.session_state.panel_admin_loaded = False
            st.session_state.dashboard_metrics_loaded = False
            st.session_state.tasks_loaded = False
            st.session_state.users_management_loaded = False
            st.success("✅ Cache limpiado")
            st.rerun()
    
    # Crear tabs para organizar funcionalidades
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🏢 Panel Admin", 
        "➕ Registrar Institución", 
        "🔍 Buscar/Editar", 
        "📊 Dashboard", 
        "📋 Tareas & Alertas",
        "👥 Gestión Usuarios"
    ])
    
    # Obtener filtros del sidebar
    filter_stage = st.session_state.get('filter_stage', [])
    filter_medium = st.session_state.get('filter_medium', [])
    filter_pais = st.session_state.get('filter_pais', [])
    filter_ciudad = st.session_state.get('filter_ciudad', [])
    
    with tab1:
        show_panel_admin(filter_stage, filter_medium, filter_pais, filter_ciudad)
    
    with tab2:
        show_registrar_institucion()
    
    with tab3:
        show_buscar_editar()
    
    with tab4:
        show_dashboard_metrics()
    
    with tab5:
        show_tareas_alertas()
    
    with tab6:
        show_gestion_usuarios()

def show_panel_admin(filter_stage, filter_medium, filter_pais, filter_ciudad):
    """Panel Admin - Kanban board con ciclo de vida de leads con carga lazy"""
    
    st.header('Panel Admin — Ciclo de vida de leads')
    
    # Inicializar estado de carga del panel admin
    if 'panel_admin_loaded' not in st.session_state:
        st.session_state.panel_admin_loaded = False
    
    if not st.session_state.panel_admin_loaded:
        #st.info('🏢 **Panel Admin Kanban Disponible**')
        st.markdown('El panel muestra todas las instituciones organizadas por etapa del proceso de ventas')
        
        # Mostrar información sobre filtros activos si los hay
        active_filters = []
        if filter_stage: active_filters.append(f"Etapas: {', '.join(filter_stage)}")
        if filter_medium: active_filters.append(f"Medios: {', '.join(filter_medium)}")
        if filter_pais: active_filters.append(f"Países: {', '.join(filter_pais)}")
        if filter_ciudad: active_filters.append(f"Ciudades: {', '.join(filter_ciudad)}")
        
        if active_filters:
            st.warning(f"🔍 **Filtros activos:** {' | '.join(active_filters)}")
        
        if st.button('Cargar Panel Kanban', use_container_width=True):
            st.session_state.panel_admin_loaded = True
            st.rerun()
        return
    
    # Build optimized WHERE clause based on filters
    where_conditions = []
    if filter_stage:
        stage_list = "','".join(filter_stage)
        where_conditions.append(f"stage IN ('{stage_list}')")
    if filter_medium:
        medium_list = "','".join(filter_medium)
        where_conditions.append(f"initial_contact_medium IN ('{medium_list}')")
    if filter_pais:
        pais_list = "','".join(filter_pais)
        where_conditions.append(f"pais IN ('{pais_list}')")
    if filter_ciudad:
        ciudad_list = "','".join(filter_ciudad)
        where_conditions.append(f"ciudad IN ('{ciudad_list}')")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else None
    
    # Configuración avanzada de paginación
    if 'items_per_stage' not in st.session_state:
        st.session_state.items_per_stage = 10  # Aumentar default a 10
    if 'current_page' not in st.session_state:
        st.session_state.current_page = {stage: 1 for stage in ['En cola','En Proceso','Ganado','No interesado']}
    if 'pagination_mode' not in st.session_state:
        st.session_state.pagination_mode = 'paginas'  # 'paginas' o 'incremental'
    
    # Controles de paginación y configuración
    with st.expander('⚙️ Configuración de Vista', expanded=False):
        
        
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        
        with col1:
            # Modo de paginación
            pagination_mode = st.radio(
                "📊 Modo de navegación",
                ["paginas", "incremental", "todo"],
                format_func=lambda x: {
                    "paginas": "📑 Páginas (Navegar por páginas)",
                    "incremental": "➕ Incremental (Mostrar más)",
                    "todo": "📈 Ver Todas (Cuidado: puede ser lento)"
                }[x],
                horizontal=True,
                help="Páginas: Navega con botones anterior/siguiente. Incremental: Carga más elementos gradualmente."
            )
            st.session_state.pagination_mode = pagination_mode
        
        with col2:
            if pagination_mode in ['paginas', 'incremental']:
                items_per_stage = st.selectbox(
                    "📊 Por etapa", 
                    [5, 10, 15, 20, 25, 50], 
                    index=1,
                    help="Cuántas instituciones mostrar por etapa"
                )
                st.session_state.items_per_stage = items_per_stage
            else:
                st.info("Modo: Ver todas")
        
        with col3:
            if st.button("🔄 Actualizar"):
                # Reset páginas al actualizar
                st.session_state.current_page = {stage: 1 for stage in ['En cola','En Proceso','Ganado','No interesado']}
                st.rerun()
        
        with col4:
            if st.button("📈 Solo Resumen"):
                st.session_state.show_summary_only = not st.session_state.get('show_summary_only', False)
                st.rerun()
    
    # Fetch only necessary data with filters applied at database level
    with st.spinner('⏳ Cargando vista optimizada...'):
        # Primero obtener conteos para cada etapa
        conn = get_conn()
        count_query = "SELECT stage, COUNT(*) as count FROM institutions"
        if where_clause:
            count_query += f" WHERE {where_clause}"
        count_query += " GROUP BY stage"
        
        stage_counts = pd.read_sql_query(count_query, conn)
        conn.close()
        
        # Solo cargar datos detallados si no está en modo resumen
        if not st.session_state.get('show_summary_only', False):
            conn = get_conn()
            
            if pagination_mode == "todo":
                # Cargar todas las instituciones (sin límite)
                query = "SELECT * FROM institutions"
                if where_clause:
                    query += f" WHERE {where_clause}"
                query += " ORDER BY stage, last_interaction DESC"
                df = pd.read_sql_query(query, conn)
                # Convertir fechas de forma segura
                if not df.empty:
                    for col in ['last_interaction', 'created_contact', 'contract_start_date', 'contract_end_date']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            elif pagination_mode == "incremental":
                # Para modo incremental, cargar más datos de los necesarios para todas las páginas actuales
                max_page = max(st.session_state.current_page.values())
                total_limit = items_per_stage * 4 * max_page  # Suficiente para todas las etapas y páginas
                
                query = "SELECT * FROM institutions"
                if where_clause:
                    query += f" WHERE {where_clause}"
                query += f" ORDER BY stage, last_interaction DESC LIMIT {total_limit}"
                df = pd.read_sql_query(query, conn)
                # Convertir fechas de forma segura
                if not df.empty:
                    for col in ['last_interaction', 'created_contact', 'contract_start_date', 'contract_end_date']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            else:  # modo "paginas"
                # Para modo páginas, cargar datos por etapa con OFFSET
                all_dfs = []
                for stage in ['En cola','En Proceso','Ganado','No interesado']:
                    page = st.session_state.current_page[stage]
                    offset = (page - 1) * items_per_stage
                    
                    stage_query = "SELECT * FROM institutions WHERE stage = ?"
                    params = [stage]
                    
                    if where_clause:
                        stage_query += f" AND ({where_clause})"
                    
                    stage_query += f" ORDER BY last_interaction DESC LIMIT {items_per_stage} OFFSET {offset}"
                    stage_df = pd.read_sql_query(stage_query, conn, params=params)
                    # Convertir fechas de forma segura
                    if not stage_df.empty:
                        for col in ['last_interaction', 'created_contact', 'contract_start_date', 'contract_end_date']:
                            if col in stage_df.columns:
                                stage_df[col] = pd.to_datetime(stage_df[col], errors='coerce')
                    all_dfs.append(stage_df)
                
                df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
            
            conn.close()
        else:
            df = pd.DataFrame()  # DataFrame vacío para modo resumen
    
    # Mostrar resumen de conteos por etapa
    with st.expander("📊 Resumen por Etapas"):
        cols = st.columns([1,1,1,1])
        stages = ['En cola','En Proceso','Ganado','No interesado']
        
        for col, stage_name in zip(cols, stages):
            with col:
                count = stage_counts[stage_counts['stage'] == stage_name]['count'].iloc[0] if len(stage_counts[stage_counts['stage'] == stage_name]) > 0 else 0
                st.metric(stage_name, count)
    
    # Solo mostrar detalles si no está en modo resumen y hay datos
    if not st.session_state.get('show_summary_only', False) and not df.empty:
        st.markdown("---")
        st.subheader("🏢 Vista Detallada por Etapas")
        
        cols = st.columns([1,1,1,1])
        
        for col, stage_name in zip(cols, stages):
            with col:
                # Encabezado con información de paginación
                stage_df = df[df['stage']==stage_name]
                total_in_stage = stage_counts[stage_counts['stage'] == stage_name]['count'].iloc[0] if len(stage_counts[stage_counts['stage'] == stage_name]) > 0 else 0
                
                if pagination_mode == "paginas" and total_in_stage > 0:
                    current_page = st.session_state.current_page[stage_name]
                    total_pages = (total_in_stage + items_per_stage - 1) // items_per_stage  # Redondear hacia arriba
                    st.subheader(f"{stage_name} (Pág. {current_page}/{total_pages})")
                elif pagination_mode == "incremental":
                    current_showing = len(stage_df)
                    st.subheader(f"{stage_name} ({current_showing}/{total_in_stage})")
                else:
                    st.subheader(f"{stage_name} ({total_in_stage})")
                
                # Mostrar instituciones
                if stage_df.empty:
                    st.info(f"No hay instituciones en '{stage_name}'")
                else:
                    # Para modo incremental, aplicar límite de visualización
                    if pagination_mode == "incremental":
                        current_page = st.session_state.current_page[stage_name]
                        display_limit = items_per_stage * current_page
                        stage_df_display = stage_df.head(display_limit)
                    else:
                        stage_df_display = stage_df
                    
                    for i, row in stage_df_display.iterrows():
                        # Vista compacta por defecto, expansión bajo demanda
                        with st.expander(f"🏢 {row['name'][:30]}{'...' if len(row['name']) > 30 else ''}", expanded=False):
                            # Información básica siempre visible
                            st.markdown(f"**📧 Contacto:** {row.get('rector_name', 'N/A')}")
                            st.markdown(f"**📅 Última interacción:** {safe_date_display(row['last_interaction'])}")
                            st.markdown(f"**🌍 Ubicación:** {row.get('pais', 'N/A')}, {row.get('ciudad', 'N/A')}")
                            
                            # Botón para cargar formulario completo
                            if st.button(f"✏️ Editar {row['name'][:20]}", key=f"edit_{row['id']}"):
                                st.session_state[f"editing_institution_{row['id']}"] = True
                                st.rerun()
                            
                            # Mostrar formulario si está en modo edición
                            if st.session_state.get(f"editing_institution_{row['id']}", False):
                                render_full_edit_form(row)
                
                # Controles de navegación por etapa
                if not stage_df.empty and total_in_stage > 0:
                    render_stage_navigation(stage_name, total_in_stage, items_per_stage, pagination_mode)
    elif not st.session_state.get('show_summary_only', False):
        st.info('ℹ️ No hay instituciones que coincidan con los filtros aplicados')
    
    # Estadísticas de navegación y controles globales
    if not st.session_state.get('show_summary_only', False) and not df.empty and pagination_mode != "todo":
        st.markdown('---')
        st.subheader("📊 Estado de Navegación")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_showing = len(df)
            total_institutions = sum(stage_counts['count'])
            st.metric("📋 Mostrando", f"{total_showing} de {total_institutions}")
        
        with col2:
            if pagination_mode == "paginas":
                avg_page = sum(st.session_state.current_page.values()) / 4
                st.metric("📄 Página promedio", f"{avg_page:.1f}")
            else:  # incremental
                avg_level = sum(st.session_state.current_page.values()) / 4
                st.metric("📈 Nivel promedio", f"{avg_level:.1f}")
        
        with col3:
            progress = (total_showing / total_institutions) * 100 if total_institutions > 0 else 0
            st.metric("📊 Progreso vista", f"{progress:.1f}%")
        
        # Controles globales de navegación
        st.markdown("**🎮 Controles Globales:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("⏮️ Todas a página 1", help="Resetear todas las etapas a la primera página"):
                st.session_state.current_page = {stage: 1 for stage in ['En cola','En Proceso','Ganado','No interesado']}
                st.rerun()
        
        with col2:
            if st.button("⏭️ Avanzar todas", help="Avanzar una página/nivel en todas las etapas"):
                for stage in st.session_state.current_page:
                    st.session_state.current_page[stage] += 1
                st.rerun()
        
        with col3:
            if pagination_mode == "incremental":
                if st.button("🚀 Cargar más en todas", help="Mostrar más elementos en todas las etapas"):
                    for stage in st.session_state.current_page:
                        st.session_state.current_page[stage] += 1
                    st.rerun()

    # Botón para cerrar el panel admin
    st.markdown('---')
    if st.button('🧹 Cerrar Panel Kanban'):
        st.session_state.panel_admin_loaded = False
        # Limpiar estados relacionados
        if 'show_summary_only' in st.session_state:
            del st.session_state.show_summary_only
        # Resetear paginación
        st.session_state.current_page = {stage: 1 for stage in ['En cola','En Proceso','Ganado','No interesado']}
        st.rerun()

def render_stage_navigation(stage_name, total_in_stage, items_per_stage, pagination_mode):
    """Renderiza los controles de navegación para cada etapa"""
    
    if pagination_mode == "paginas":
        current_page = st.session_state.current_page[stage_name]
        total_pages = (total_in_stage + items_per_stage - 1) // items_per_stage
        
        if total_pages > 1:
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if current_page > 1:
                    if st.button(f"⬅️", key=f"prev_{stage_name}", help="Página anterior"):
                        st.session_state.current_page[stage_name] -= 1
                        st.rerun()
            
            with col2:
                st.markdown(f"<div style='text-align: center'>Página {current_page} de {total_pages}</div>", unsafe_allow_html=True)
            
            with col3:
                if current_page < total_pages:
                    if st.button(f"➡️", key=f"next_{stage_name}", help="Página siguiente"):
                        st.session_state.current_page[stage_name] += 1
                        st.rerun()
    
    elif pagination_mode == "incremental":
        current_page = st.session_state.current_page[stage_name]
        current_showing = items_per_stage * current_page
        
        if current_showing < total_in_stage:
            remaining = total_in_stage - current_showing
            next_batch = min(items_per_stage, remaining)
            if st.button(f"➕ Mostrar {next_batch} más", key=f"more_{stage_name}", use_container_width=True):
                st.session_state.current_page[stage_name] += 1
                st.rerun()
        
        # Botón para resetear vista
        if current_page > 1:
            if st.button(f"🔄 Volver al inicio", key=f"reset_{stage_name}", use_container_width=True):
                st.session_state.current_page[stage_name] = 1
                st.rerun()

def render_full_edit_form(row):
    """Renderiza el formulario completo de edición para una institución específica"""
    st.markdown(f"### ✏️ Editando: {row['name']}")
    
    # Inicializar valores en session_state si no existen
    if f"form_data_{row['id']}" not in st.session_state:
        st.session_state[f"form_data_{row['id']}"] = {
            'name': row['name'],
            'stage': row.get('stage', 'En cola'),
            'substage': row.get('substage', 'Primera reunión'),
            'initial_contact_medium': row.get('initial_contact_medium', 'Whatsapp'),
            'program_proposed': row.get('program_proposed', 'Demo')
        }
    
    # Dividir en tabs para mejor organización
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Info Básica", "🎯 Pipeline", "📝 Tareas", "💼 Contrato"])
    
    with tab1:
        st.markdown("#### 🏢 Información de la Institución")
        name_edit = st.text_input('Nombre de la institución*', value=row['name'], key=f"name_{row['id']}")
        
        col1, col2 = st.columns(2)
        with col1:
            website_edit = st.text_input('Página web', value=row.get('website', ''), key=f"web_{row['id']}")
            pais_edit = st.selectbox('País*', 
                options=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'], 
                index=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'].index(row.get('pais', 'Ecuador')) if row.get('pais') in ['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'] else 0, 
                key=f"pais_{row['id']}")
        with col2:
            ciudad_edit = st.text_input('Ciudad', value=row.get('ciudad', ''), key=f"ciudad_{row['id']}")
            direccion_edit = st.text_input('Dirección', value=row.get('direccion', ''), key=f"direccion_{row['id']}")
        
        st.markdown("#### 👥 Información Académica")
        col1, col2, col3 = st.columns(3)
        with col1:
            num_teachers_edit = st.number_input('Número de docentes', min_value=0, step=1, 
                value=int(row.get('num_teachers', 0)) if not pd.isna(row.get('num_teachers', 0)) else 0, 
                key=f"teachers_{row['id']}")
        with col2:
            num_students_edit = st.number_input('Número de estudiantes', min_value=0, step=1, 
                value=int(row.get('num_students', 0)) if not pd.isna(row.get('num_students', 0)) else 0, 
                key=f"students_{row['id']}")
        with col3:
            avg_fee_edit = st.number_input('Pensión promedio', min_value=0.0, format="%.2f", 
                value=float(row.get('avg_fee', 0)) if not pd.isna(row.get('avg_fee', 0)) else 0.0, 
                key=f"fee_{row['id']}")
        
        st.markdown("#### 📞 Contactos")
        
        # Rector
        st.markdown("**👨‍💼 Rector (Obligatorio)**")
        col1, col2 = st.columns(2)
        with col1:
            rector_name_edit = st.text_input('Nombre del Rector*', value=row.get('rector_name', ''), key=f"rector_name_{row['id']}")
            rector_email_edit = st.text_input('Email del Rector*', value=row.get('rector_email', ''), key=f"rector_email_{row['id']}")
        with col2:
            current_rector_phone = str(row.get('rector_phone', ''))
            rector_country_options = ['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina']
            rector_country_index = 0
            for idx, option in enumerate(rector_country_options):
                if option.split(' ')[1] in current_rector_phone:
                    rector_country_index = idx
                    break
            rector_country_code_edit = st.selectbox('País Rector', 
                options=rector_country_options, 
                index=rector_country_index,
                key=f"rector_country_{row['id']}")
            rector_phone_only = current_rector_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
            rector_phone_edit = st.text_input('Celular Rector* (sin código país)', value=rector_phone_only, key=f"rector_phone_{row['id']}", placeholder='987654321')
        
        # Contraparte
        st.markdown("**🤝 Contraparte (Obligatorio)**")
        col1, col2 = st.columns(2)
        with col1:
            contraparte_name_edit = st.text_input('Nombre Contraparte*', value=row.get('contraparte_name', ''), key=f"contraparte_name_{row['id']}")
            contraparte_email_edit = st.text_input('Email Contraparte*', value=row.get('contraparte_email', ''), key=f"contraparte_email_{row['id']}")
        with col2:
            current_contraparte_phone = str(row.get('contraparte_phone', ''))
            contraparte_country_index = 0
            for idx, option in enumerate(rector_country_options):
                if option.split(' ')[1] in current_contraparte_phone:
                    contraparte_country_index = idx
                    break
            contraparte_country_code_edit = st.selectbox('País Contraparte', 
                options=rector_country_options, 
                index=contraparte_country_index,
                key=f"contraparte_country_{row['id']}")
            contraparte_phone_only = current_contraparte_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
            contraparte_phone_edit = st.text_input('Celular Contraparte* (sin código país)', value=contraparte_phone_only, key=f"contraparte_phone_{row['id']}", placeholder='987654321')
    
    with tab2:
        st.markdown("#### 🎯 Pipeline de Ventas")
        
        col1, col2 = st.columns(2)
        with col1:
            # Usar session_state para mantener valores seleccionados
            current_stage = st.session_state[f"form_data_{row['id']}"]["stage"]
            stage_edit = st.selectbox('🏗️ Etapa*', 
                options=['En cola','En Proceso','Ganado','No interesado'], 
                index=['En cola','En Proceso','Ganado','No interesado'].index(current_stage) if current_stage in ['En cola','En Proceso','Ganado','No interesado'] else 0, 
                key=f"stage_{row['id']}",
                on_change=lambda: st.session_state[f"form_data_{row['id']}"].update({'stage': st.session_state[f"stage_{row['id']}"]}))
            
            substage_options = ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido']
            current_substage = st.session_state[f"form_data_{row['id']}"]["substage"]
            substage_edit = st.selectbox('📊 Subetapa', 
                options=substage_options, 
                index=substage_options.index(current_substage) if current_substage in substage_options else 0,
                key=f"substage_{row['id']}",
                on_change=lambda: st.session_state[f"form_data_{row['id']}"].update({'substage': st.session_state[f"substage_{row['id']}"]}))
        
        with col2:
            initial_contact_medium_options = ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales']
            current_medium = st.session_state[f"form_data_{row['id']}"]["initial_contact_medium"]
            initial_contact_medium_edit = st.selectbox('📞 Medio de contacto inicial', 
                options=initial_contact_medium_options, 
                index=initial_contact_medium_options.index(current_medium) if current_medium in initial_contact_medium_options else 0,
                key=f"medium_{row['id']}",
                on_change=lambda: st.session_state[f"form_data_{row['id']}"].update({'initial_contact_medium': st.session_state[f"medium_{row['id']}"]}))
            
            # Obtener usuarios disponibles para el selector
            user_options, user_mapping = get_available_users()
            
            # Encontrar el usuario actual en la lista de opciones
            current_assigned = row.get('assigned_commercial', '')
            current_index = 0  # Por defecto "Sin asignar"
            
            if current_assigned:
                # Buscar el usuario actual en las opciones disponibles
                for i, option in enumerate(user_options):
                    if option in user_mapping and user_mapping[option] == current_assigned:
                        current_index = i
                        break
            
            assigned_commercial_display = st.selectbox('👤 Responsable comercial', 
                options=user_options, 
                index=current_index,
                key=f"assign_{row['id']}")
            assigned_commercial_edit = user_mapping[assigned_commercial_display]
        
        st.markdown("#### 💰 Propuesta Comercial")
        col1, col2 = st.columns(2)
        with col1:
            program_options = ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo']
            current_program = st.session_state[f"form_data_{row['id']}"]["program_proposed"]
            program_proposed_edit = st.selectbox('🎓 Programa propuesto', 
                options=program_options, 
                index=program_options.index(current_program) if current_program in program_options else 0,
                key=f"program_{row['id']}",
                on_change=lambda: st.session_state[f"form_data_{row['id']}"].update({'program_proposed': st.session_state[f"program_{row['id']}"]}))
        with col2:
            proposal_value_edit = st.number_input('💵 Valor propuesta', min_value=0.0, format="%.2f", 
                value=float(row.get('proposal_value', 0)) if not pd.isna(row.get('proposal_value', 0)) else 0.0, 
                key=f"proposal_{row['id']}")
        
        st.markdown("#### 📅 Fechas Importantes")
        col1, col2 = st.columns(2)
        with col1:
            created_contact_edit = st.date_input('📞 Fecha primer contacto', 
                value=safe_date_value(row.get('created_contact')), key=f"created_contact_{row['id']}")
        with col2:
            last_interaction_edit = st.date_input('🕐 Última interacción', 
                value=safe_date_value(row.get('last_interaction')), key=f"last_interaction_{row['id']}")
        
        observations_edit = st.text_area('📝 Observaciones/Notas', value=row.get('observations', ''), key=f"observaciones_{row['id']}")
    
    with tab3:
        st.markdown("#### 📝 Gestión de Tareas")
        
        # Mostrar tareas existentes - evitar conversión automática de fechas
        try:
            conn = sqlite3.connect(DB_PATH, detect_types=0)  # No auto-conversión de tipos
            c = conn.cursor()
            c.execute('''
                SELECT id, title, 
                       CAST(due_date AS TEXT) as due_date_str, 
                       done, notes, 
                       CAST(created_at AS TEXT) as created_at_str
                FROM tasks WHERE institution_id = ?
                ORDER BY id DESC
            ''', (row['id'],))
            
            task_rows = c.fetchall()
            conn.close()
            
            # Crear DataFrame manualmente
            if task_rows:
                existing_tasks = pd.DataFrame(task_rows, columns=['id', 'title', 'due_date', 'done', 'notes', 'created_at'])
                # Convertir fechas de forma muy segura
                for col in ['due_date', 'created_at']:
                    if col in existing_tasks.columns:
                        # Limpiar datos antes de convertir
                        existing_tasks[col] = existing_tasks[col].astype(str)
                        existing_tasks[col] = existing_tasks[col].str.replace(r'[^\d\-:\s]', '', regex=True)
                        existing_tasks[col] = pd.to_datetime(existing_tasks[col], errors='coerce')
            else:
                existing_tasks = pd.DataFrame(columns=['id', 'title', 'due_date', 'done', 'notes', 'created_at'])
                
        except Exception as e:
            st.warning(f"⚠️ Problema al cargar tareas: {str(e)}")
            existing_tasks = pd.DataFrame(columns=['id', 'title', 'due_date', 'done', 'notes', 'created_at'])
        
        if not existing_tasks.empty:
            st.markdown("**📋 Tareas Existentes:**")
            for idx, task in existing_tasks.iterrows():
                col1, col2, col3 = st.columns([6, 1, 1])
                with col1:
                    status_icon = "✅" if task['done'] else "⏳"
                    st.write(f"{status_icon} **{task['title']}** - Vence: {safe_date_display(task['due_date'])}")
                    if task['notes']:
                        # Extraer información del responsable de las notas para mejor visualización
                        notes_text = str(task['notes'])
                        if "Responsable:" in notes_text:
                            lines = notes_text.split('\n')
                            task_notes = []
                            responsable_info = []
                            
                            for line in lines:
                                if line.startswith('Responsable:') or line.startswith('Email:') or line.startswith('Rol:'):
                                    responsable_info.append(line)
                                elif line.strip():  # Solo líneas no vacías que no sean info del responsable
                                    task_notes.append(line)
                            
                            if task_notes:
                                st.caption(f"📝 {' '.join(task_notes)}")
                            if responsable_info:
                                st.caption(f"👤 {' | '.join(responsable_info)}")
                        else:
                            st.caption(f"📝 {notes_text}")
                with col2:
                    if st.button("✏️", key=f"edit_form_edit_task_{task['id']}", help="Editar tarea"):
                        st.session_state[f"editing_task_{task['id']}"] = True
                with col3:
                    if st.button("🗑️", key=f"edit_form_del_task_{task['id']}", help="Eliminar tarea"):
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute('DELETE FROM tasks WHERE id=?', (task['id'],))
                        conn.commit()
                        conn.close()
                        st.success("✅ Tarea eliminada")
                        st.rerun()
        
        # Crear nueva tarea
        st.markdown("**➕ Crear Nueva Tarea:**")
        with st.form(key=f"new_task_{row['id']}"):
            col1, col2 = st.columns(2)
            with col1:
                task_title = st.text_input('📋 Título de la tarea*')
                task_due_date = st.date_input('📅 Fecha de vencimiento', value=now_date())
            with col2:
                # Obtener usuarios de sales y support
                user_options, user_data = get_sales_support_users()
                
                if user_options:
                    # Agregar opción "Sin asignar" al inicio
                    user_options_with_none = ["Sin asignar"] + user_options
                    selected_user = st.selectbox('� Responsable*', 
                                               options=user_options_with_none,
                                               help="Selecciona un usuario con rol Sales o Support")
                    
                    # Mostrar información del usuario seleccionado
                    if selected_user != "Sin asignar" and selected_user in user_data:
                        user_info = user_data[selected_user]
                        st.caption(f"📧 {user_info['email']}")
                else:
                    st.warning("⚠️ No hay usuarios activos con rol Sales o Support")
                    selected_user = "Sin asignar"
            
            task_notes = st.text_area('� Notas de la tarea')
            
            if st.form_submit_button('➕ Crear Tarea'):
                if task_title:
                    # Preparar datos del responsable
                    if selected_user != "Sin asignar" and selected_user in user_data:
                        user_info = user_data[selected_user]
                        responsable_info = f"Responsable: {user_info['full_name'] or user_info['username']} ({user_info['username']})\nEmail: {user_info['email']}\nRol: {user_info['role'].title()}"
                    else:
                        responsable_info = "Responsable: Sin asignar"
                    
                    full_notes = f"{task_notes}\n\n{responsable_info}"
                    create_task(row['id'], task_title, task_due_date, full_notes)
                    st.success('✅ Tarea creada correctamente')
                    st.rerun()
                else:
                    st.error("❌ El título de la tarea es obligatorio")
    
    with tab4:
        st.markdown("#### 💼 Información de Contrato")
        
        col1, col2 = st.columns(2)
        with col1:
            contract_start_date_edit = st.date_input('📅 Inicio de contrato', 
                value=safe_date_value(row.get('contract_start_date')), key=f"contract_start_{row['id']}")
        with col2:
            contract_end_date_edit = st.date_input('📅 Fin de contrato', 
                value=safe_date_value(row.get('contract_end_date')), key=f"contract_end_{row['id']}")
        
        # Razón de no interés (si aplica)
        if st.session_state[f"form_data_{row['id']}"]["stage"] == "No interesado":
            no_interest_reason_edit = st.text_area('❌ Razón de no interés', 
                value=row.get('no_interest_reason', ''), key=f"no_interest_{row['id']}")
        else:
            no_interest_reason_edit = None
    
    # Botones de acción
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button('💾 Guardar Todos los Cambios', type='primary', key=f"save_all_{row['id']}", use_container_width=True):
            save_institution_changes(row['id'], {
                'name': name_edit,
                'website': website_edit,
                'pais': pais_edit,
                'ciudad': ciudad_edit,
                'direccion': direccion_edit,
                'num_teachers': num_teachers_edit,
                'num_students': num_students_edit,
                'avg_fee': avg_fee_edit,
                'rector_name': rector_name_edit,
                'rector_email': rector_email_edit,
                'rector_phone': rector_country_code_edit.split(' ')[1] + ' ' + rector_phone_edit,
                'contraparte_name': contraparte_name_edit,
                'contraparte_email': contraparte_email_edit,
                'contraparte_phone': contraparte_country_code_edit.split(' ')[1] + ' ' + contraparte_phone_edit,
                'stage': stage_edit,
                'substage': substage_edit,
                'initial_contact_medium': initial_contact_medium_edit,
                'assigned_commercial': assigned_commercial_edit,
                'program_proposed': program_proposed_edit,
                'proposal_value': proposal_value_edit,
                'created_contact': created_contact_edit,
                'last_interaction': last_interaction_edit,
                'observations': observations_edit,
                'contract_start_date': contract_start_date_edit,
                'contract_end_date': contract_end_date_edit,
                'no_interest_reason': no_interest_reason_edit
            })
            st.success(f"✅ Todos los cambios guardados para {name_edit}")
            st.balloons()
            # Limpiar session state del formulario después de guardar
            if f"form_data_{row['id']}" in st.session_state:
                del st.session_state[f"form_data_{row['id']}"]
            if f"editing_institution_{row['id']}" in st.session_state:
                del st.session_state[f"editing_institution_{row['id']}"]
            st.rerun()
    
    with col2:
        if st.button('🔄 Recargar Datos', key=f"reload_{row['id']}", use_container_width=True):
            st.rerun()
    
    with col3:
        if st.button('❌ Cerrar', key=f"close_{row['id']}", use_container_width=True):
            # Cerrar el formulario de edición
            if f"editing_institution_{row['id']}" in st.session_state:
                del st.session_state[f"editing_institution_{row['id']}"]
            st.rerun()

def save_institution_changes(institution_id, changes):
    """Guarda los cambios de una institución en la base de datos"""
    try:
        from pytz import timezone
        tz = timezone('America/Guayaquil')
        now_ecuador = datetime.now(tz)
        
        conn = get_conn()
        c = conn.cursor()
        
        # Función auxiliar para manejar valores seguros
        def safe_int(val):
            try:
                return int(val) if val is not None else 0
            except (ValueError, TypeError):
                return 0
        
        def safe_float(val):
            try:
                return float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                return 0.0
        
        c.execute('''
            UPDATE institutions SET 
                name=?, website=?, pais=?, ciudad=?, direccion=?, 
                num_teachers=?, num_students=?, avg_fee=?, 
                rector_name=?, rector_email=?, rector_phone=?, 
                contraparte_name=?, contraparte_email=?, contraparte_phone=?, 
                stage=?, substage=?, initial_contact_medium=?, assigned_commercial=?, 
                program_proposed=?, proposal_value=?, created_contact=?, last_interaction=?, 
                observations=?, contract_start_date=?, contract_end_date=?, no_interest_reason=?,
                last_interaction=?
            WHERE id=?
        ''', (
            changes['name'], changes['website'], changes['pais'], changes['ciudad'], changes['direccion'],
            safe_int(changes['num_teachers']), safe_int(changes['num_students']), safe_float(changes['avg_fee']),
            changes['rector_name'], changes['rector_email'], changes['rector_phone'],
            changes['contraparte_name'], changes['contraparte_email'], changes['contraparte_phone'],
            changes['stage'], changes['substage'], changes['initial_contact_medium'], changes['assigned_commercial'],
            changes['program_proposed'], safe_float(changes['proposal_value']), 
            str(changes['created_contact']) if changes['created_contact'] else None,
            str(changes['last_interaction']) if changes['last_interaction'] else None,
            changes['observations'],
            str(changes['contract_start_date']) if changes['contract_start_date'] else None,
            str(changes['contract_end_date']) if changes['contract_end_date'] else None,
            changes['no_interest_reason'],
            now_ecuador,  # Actualizar timestamp de última interacción
            institution_id
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        st.error(f"❌ Error al guardar cambios: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

def create_task(institution_id, title, due_date, notes=''):
    """Crea una nueva tarea para una institución"""
    try:
        conn = get_conn()
        c = conn.cursor()
        
        task_id = str(uuid.uuid4())
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Convertir due_date a string formato fecha
        if isinstance(due_date, (datetime, date)):
            due_date_str = due_date.strftime('%Y-%m-%d')
        else:
            due_date_str = str(due_date)
        
        c.execute('''
            INSERT INTO tasks (id, institution_id, title, due_date, notes, done, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, institution_id, title, due_date_str, notes, 0, created_at))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        st.error(f"❌ Error al crear tarea: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

# Código del formulario largo movido a render_full_edit_form para optimización

def show_registrar_institucion():
    """Página para registrar nueva institución"""
    st.header('➕ Registrar nueva institución')
    
    with st.expander('📝 Formulario de registro de institución', expanded=False):
        name = st.text_input('Nombre de la institución', max_chars=200)
        
        # CONTACTO section
        st.markdown('**👤 Rector (Obligatorio)**')
        rector_name = st.text_input('Nombre del Rector*', key='rector_name_reg')
        rector_email = st.text_input('Email del Rector*', key='rector_email_reg')
        col1, col2 = st.columns([1, 2])
        with col1:
            rector_country_code = st.selectbox('País', 
                options=['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina'], 
                key='rector_country_reg')
        with col2:
            rector_phone = st.text_input('Celular del Rector* (sin código país)', key='rector_phone_reg', placeholder='987654321')
        
        st.markdown('**👥 Contraparte (Obligatorio)**')
        contraparte_name = st.text_input('Nombre de la Contraparte*', key='contraparte_name_reg')
        contraparte_email = st.text_input('Email de la Contraparte*', key='contraparte_email_reg')
        col1, col2 = st.columns([1, 2])
        with col1:
            contraparte_country_code = st.selectbox('País', 
                options=['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina'], 
                key='contraparte_country_reg')
        with col2:
            contraparte_phone = st.text_input('Celular de la Contraparte* (sin código país)', key='contraparte_phone_reg', placeholder='987654321')
        
        website = st.text_input('Página web')
        col1, col2, col3 = st.columns(3)
        with col1:
            pais = st.selectbox('País', options=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'])
        with col2:
            ciudad = st.text_input('Ciudad')
        with col3:
            direccion = st.text_input('Dirección')
        
        col1, col2 = st.columns(2)
        with col1:
            created_contact = st.date_input('Fecha de creación de contacto', value=now_date())
        with col2:
            last_interaction = st.date_input('Fecha última interacción', value=now_date())
        
        col1, col2 = st.columns(2)
        with col1:
            num_teachers = st.number_input('Número de docentes', min_value=0, step=1)
        with col2:
            num_students = st.number_input('Número de estudiantes', min_value=0, step=1)
        
        col1, col2 = st.columns(2)
        with col1:
            avg_fee = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f")
        with col2:
            initial_contact_medium = st.selectbox('Medio de contacto', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'])
        
        stage = st.selectbox('Etapa', ['En cola','En Proceso','Ganado','No interesado'])
        substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'])
        program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'])
        
        col1, col2 = st.columns(2)
        with col1:
            proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f")
        with col2:
            # Obtener usuarios disponibles
            user_options, user_mapping = get_available_users()
            assigned_commercial_display = st.selectbox('Responsable comercial', options=user_options, index=0)
            assigned_commercial = user_mapping[assigned_commercial_display]
        
        # CONTRATO section
        st.markdown('**📄 CONTRATO**')
        col1, col2 = st.columns(2)
        with col1:
            contract_start_date = st.date_input('Inicio de contrato', value=None, key='contract_start_reg')
        with col2:
            contract_end_date = st.date_input('Fin de contrato', value=None, key='contract_end_reg')
        
        observations = st.text_area('Observaciones')
        
        guardar = st.button('💾 Guardar institución', type='primary')
        
        if guardar:
            if not name:
                st.error('❌ El nombre de la institución es obligatorio')
            elif not rector_name or not rector_email or not rector_phone:
                st.error('❌ Todos los campos del Rector son obligatorios')
            elif not contraparte_name or not contraparte_email or not contraparte_phone:
                st.error('❌ Todos los campos de la Contraparte son obligatorios')
            else:
                # Extract country codes
                rector_full_phone = rector_country_code.split(' ')[1] + ' ' + rector_phone
                contraparte_full_phone = contraparte_country_code.split(' ')[1] + ' ' + contraparte_phone
                
                inst = {
                    'id': str(uuid.uuid4()),
                    'name': name,
                    'rector_name': rector_name,
                    'rector_email': rector_email,
                    'rector_phone': rector_full_phone,
                    'contraparte_name': contraparte_name,
                    'contraparte_email': contraparte_email,
                    'contraparte_phone': contraparte_full_phone,
                    'website': website,
                    'pais': pais,
                    'ciudad': ciudad,
                    'direccion': direccion,
                    'created_contact': str(created_contact),
                    'last_interaction': str(last_interaction),
                    'num_teachers': int(num_teachers),
                    'num_students': int(num_students),
                    'avg_fee': float(avg_fee),
                    'initial_contact_medium': initial_contact_medium,
                    'stage': stage,
                    'substage': substage,
                    'program_proposed': program_proposed,
                    'proposal_value': float(proposal_value),
                    'contract_start_date': str(contract_start_date) if contract_start_date else None,
                    'contract_end_date': str(contract_end_date) if contract_end_date else None,
                    'observations': observations,
                    'no_interest_reason': None,
                    'assigned_commercial': assigned_commercial
                }
                save_institution(inst)
                st.success('✅ Institución guardada correctamente')
                st.balloons()

def show_buscar_editar():
    """Página para buscar y editar instituciones optimizada"""
    st.header('Buscar o editar instituciones')
    
    q = st.text_input('Buscar por nombre, rector o email')
    
    # Only load data when there's a search query or when explicitly requested
    if q:
        # Search with database-level filtering for better performance
        search_query = f"%{q}%"
        where_clause = f"name LIKE '{search_query}' OR rector_name LIKE '{search_query}' OR rector_email LIKE '{search_query}' OR contraparte_name LIKE '{search_query}' OR contraparte_email LIKE '{search_query}'"
        results = fetch_institutions_df(where_clause=where_clause)
    else:
        # Show option to load all data or provide search hint
        if st.button("📋 Mostrar todas las instituciones", help="Cargar todas las instituciones (puede ser lento)"):
            results = fetch_institutions_df()
        else:
            st.info("💡 Ingresa un término de búsqueda para encontrar instituciones específicas o haz clic en 'Mostrar todas' para ver la lista completa")
            return
    
    if not results.empty:
        
        # Display all columns in the dataframe
        st.dataframe(results, use_container_width=True)
        
        # Select one to edit por nombre
        if not results.empty:
            name_to_id = dict(zip(results['name'], results['id']))
            sel_name = st.selectbox('Seleccionar institución para editar', options=results['name'].tolist())
            sel = name_to_id.get(sel_name) if sel_name else None
            
            if sel:
                row = results[results['id']==sel].iloc[0]
                with st.expander('Editar institución', expanded=False):
                    # Similar form as registration but with edit functionality
                    # [Rest of the edit form code would go here - truncated for space]
                    st.info("💡 Funcionalidad de edición completa disponible en el Panel Admin")
    else:
        st.info('ℹ️ No hay instituciones registradas aún')

def show_dashboard_metrics():
    """Dashboard con métricas y reportes con carga lazy real"""
    st.header('Dashboard — Métricas clave')
    
    # Mostrar botón para cargar métricas en lugar de cargarlas automáticamente
    if 'dashboard_metrics_loaded' not in st.session_state:
        st.session_state.dashboard_metrics_loaded = False
    
    if not st.session_state.dashboard_metrics_loaded:
        st.info('**Dashboard de Métricas Disponible**')
        st.markdown('Haz clic en el botón para cargar las métricas y gráficos del sistema')
        
        if st.button('Cargar Dashboard de Métricas', use_container_width=True):
            st.session_state.dashboard_metrics_loaded = True
            st.rerun()
    else:
        # Ahora sí cargar las métricas
        with st.spinner('⏳ Cargando métricas...'):
            metrics = get_institutions_metrics()
        
        if metrics['total'] == 0:
            st.info('ℹ️ No hay datos para mostrar')
            # Botón para recargar
            if st.button('🔄 Recargar Dashboard'):
                st.session_state.dashboard_metrics_loaded = False
                st.rerun()
        else:
            # Display basic metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric('Total de leads', metrics['total'])
            
            stage_counts = metrics['stage_counts']
            col2.metric('En cola', stage_counts.get('En cola', 0))
            col3.metric('En proceso', stage_counts.get('En Proceso', 0))
            col4.metric('Ganados', stage_counts.get('Ganado', 0))

            # Conversion rate: en cola -> ganados
            en_cola = stage_counts.get('En cola', 0)
            ganados = stage_counts.get('Ganado', 0)
            conv = (ganados / en_cola * 100) if en_cola > 0 else None
            st.write('📈 Tasa conversión (En cola → Ganado):', f"{conv:.1f}%" if conv is not None else 'N/A')

            # Load data only for charts when needed
            st.subheader("📊 Gráficos Detallados")
            if st.button("🔄 Cargar Gráficos Detallados"):
                with st.spinner("Cargando datos para gráficos..."):
                    # Only load data when user requests detailed charts
                    df_charts = fetch_institutions_df(
                        columns=['initial_contact_medium', 'stage', 'created_contact', 'last_interaction', 'num_teachers', 'avg_fee']
                    )
                    
                    if not df_charts.empty:
                        # Medio de contacto mas efectivo
                        st.subheader("📞 Medios de Contacto Más Efectivos")
                        med_counts = df_charts['initial_contact_medium'].value_counts()
                        if not med_counts.empty:
                            med_df = med_counts.reset_index()
                            med_df.columns = ['medium','count']
                            chart = alt.Chart(med_df).mark_bar().encode(x='medium', y='count')
                            st.altair_chart(chart, use_container_width=True)

                        # Tiempo promedio en cada etapa
                        st.subheader("⏱️ Tiempo Promedio por Etapa")
                        df_charts['days_in_pipeline'] = (df_charts['last_interaction'] - df_charts['created_contact']).dt.days
                        avg_days_by_stage = df_charts.groupby('stage')['days_in_pipeline'].mean().reset_index()
                        if not avg_days_by_stage.empty:
                            chart2 = alt.Chart(avg_days_by_stage).mark_bar().encode(x='stage', y='days_in_pipeline')
                            st.altair_chart(chart2, use_container_width=True)

                        # Valor potencial acumulado
                        st.subheader("💰 Valor Potencial")
                        df_charts['potential_value'] = df_charts['num_teachers'].fillna(0) * df_charts['avg_fee'].fillna(0)
                        total_potential = df_charts['potential_value'].sum()
                        st.metric('💰 Valor potencial acumulado (estimado)', f"${total_potential:,.2f}")
            else:
                st.info("💡 Haz clic en 'Cargar Gráficos Detallados' para ver análisis completos")
        
        # Botón para recargar/limpiar cache
        st.markdown('---')
        col1, col2 = st.columns(2)
        with col1:
            if st.button('🔄 Actualizar Métricas'):
                # Force refresh of metrics
                st.rerun()
        with col2:
            if st.button('🧹 Limpiar Dashboard'):
                st.session_state.dashboard_metrics_loaded = False
                st.rerun()

def show_tareas_alertas():
    """Gestión de tareas y alertas con carga optimizada"""
    st.header('📋 Tareas y alertas automatizadas')
    
    # Inicializar estado de carga
    if 'tasks_loaded' not in st.session_state:
        st.session_state.tasks_loaded = False
    
    if not st.session_state.tasks_loaded:
        st.info('📋 **Panel de Tareas y Alertas Disponible**')
        if st.button('📂 Cargar Tareas y Alertas', use_container_width=True):
            st.session_state.tasks_loaded = True
            st.rerun()
        return
    
    # Cargar tareas solo cuando se necesiten - sin auto-conversión de fechas
    with st.spinner('⏳ Cargando tareas...'):
        try:
            conn = sqlite3.connect(DB_PATH, detect_types=0)  # No auto-conversión de tipos
            c = conn.cursor()
            c.execute('''
                SELECT t.id, i.name as institucion, t.title, 
                       CAST(t.due_date AS TEXT) as due_date_str, 
                       t.done, 
                       CAST(t.created_at AS TEXT) as created_at_str, 
                       t.notes
                FROM tasks t LEFT JOIN institutions i ON t.institution_id = i.id
                ORDER BY t.id DESC
            ''')
            
            task_rows = c.fetchall()
            conn.close()
            
            # Crear DataFrame manualmente
            if task_rows:
                tasks = pd.DataFrame(task_rows, columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
                # Convertir fechas de forma segura
                for col in ['due_date', 'created_at']:
                    if col in tasks.columns:
                        # Limpiar datos antes de convertir
                        tasks[col] = tasks[col].astype(str)
                        tasks[col] = tasks[col].str.replace(r'[^\d\-:\s]', '', regex=True)
                        tasks[col] = pd.to_datetime(tasks[col], errors='coerce')
            else:
                tasks = pd.DataFrame(columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
                
        except Exception as e:
            st.error(f"❌ Error al cargar tareas: {str(e)}")
            tasks = pd.DataFrame(columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
    
    if tasks.empty:
        st.info('ℹ️ No hay tareas registradas')
    else:
        for idx, row in tasks.iterrows():
            # Expandir tarjeta para incluir botones de envío
            with st.expander(f"📋 {row['title']} — {row['institucion']}", expanded=False):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write(f"**Vence:** {row['due_date'].date() if not pd.isna(row['due_date']) else 'N/A'}")
                    st.write(f"**Creada:** {row['created_at'].date() if not pd.isna(row['created_at']) else 'N/A'}")
                    st.write(f"**Notas:** {row['notes'] or 'Sin notas'}")
                    
                    # Extraer y mostrar información del responsable
                    responsable_info = extract_responsable_info_from_notes(row['notes'])
                    if responsable_info:
                        st.write(f"**👤 Responsable:** {responsable_info.get('full_name', 'N/A')} ({responsable_info.get('username', 'N/A')})")
                        st.write(f"**📧 Email:** {responsable_info.get('email', 'N/A')}")
                        st.write(f"**🏷️ Rol:** {responsable_info.get('role', 'N/A')}")
                
                with col2:
                    # Checkbox para marcar como done
                    checked = st.checkbox('✅ Completada', value=bool(row['done']), key=f"dashboard_done_{row['id']}")
                    if checked != bool(row['done']):
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute('UPDATE tasks SET done=? WHERE id=?', (int(checked), row['id']))
                        conn.commit()
                        conn.close()
                        st.rerun()
                    
                    # Botón eliminar
                    if st.button('🗑️ Eliminar', key=f'dashboard_del_task_{row["id"]}', use_container_width=True):
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute('DELETE FROM tasks WHERE id=?', (row['id'],))
                        conn.commit()
                        conn.close()
                        st.success('✅ Tarea eliminada')
                        st.rerun()
                
                # Sección de envío de notificaciones
                if responsable_info and responsable_info.get('email'):
                    st.markdown("**📤 Enviar Notificación al Responsable:**")
                    
                    # Verificar configuración de email
                    if ADMIN_EMAIL == "tu_email@gmail.com":
                        st.warning("⚠️ **Configuración necesaria**: Para enviar emails, configura ADMIN_EMAIL y ADMIN_APP_PASSWORD en admin_dashboard.py (líneas 24-25)")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button('📧 Enviar Email', key=f'email_task_{row["id"]}', use_container_width=True):
                            with st.spinner('📧 Enviando email...'):
                                success, message = send_task_email(row, responsable_info)
                                if success:
                                    st.success(message)
                                    st.balloons()
                                else:
                                    st.error(message)
                    
                    with col2:
                        if st.button('💬 Enviar WhatsApp', key=f'whatsapp_task_{row["id"]}', use_container_width=True):
                            success, result = send_task_whatsapp(row, responsable_info)
                            if success:
                                st.success("✅ Abriendo WhatsApp...")
                                st.markdown(f"[🔗 Abrir WhatsApp]({result})")
                                # Abrir en nueva ventana del navegador
                                st.components.v1.html(
                                    f'<script>window.open("{result}", "_blank");</script>',
                                    height=0
                                )
                            else:
                                st.error(f"❌ {result}")
                    
                    with col3:
                        st.info("💡 Los mensajes incluyen todos los detalles de la tarea")
                else:
                    st.warning("⚠️ No se puede enviar notificación: falta información del responsable")

    # Alerts: leads without contact > 7 días (optimized with database query)
    st.subheader("⚠️ Alertas de Seguimiento")
    conn = get_conn()
    try:
        # Get stale leads directly from database with date calculation
        stale_query = '''
            SELECT id, name, last_interaction, assigned_commercial
            FROM institutions 
            WHERE last_interaction < datetime('now', '-7 days')
            ORDER BY last_interaction ASC
        '''
        stale_df = pd.read_sql_query(stale_query, conn)
        # Convertir fechas de forma segura
        if not stale_df.empty:
            stale_df['last_interaction'] = pd.to_datetime(stale_df['last_interaction'], errors='coerce')
        
        if not stale_df.empty:
            st.warning(f'⚠️ {len(stale_df)} leads sin contacto > 7 días:')
            for i, row in stale_df.iterrows():
                st.write(f"{row['name']} — Última interacción: {row['last_interaction'].date() if not pd.isna(row['last_interaction']) else 'N/A'} — Responsable: {row.get('assigned_commercial', 'No asignado')}")
                if st.button(f'📝 Marcar tarea de seguimiento', key=f'follow_{row["id"]}'):
                    create_task(row['id'], 'Seguimiento - Lead sin contacto >7d', pd.Timestamp.now().date() + timedelta(days=1), notes='Generado desde alerta')
                    st.success('✅ Tarea creada')
                    st.rerun()
        else:
            st.success("✅ Todos los leads tienen contacto reciente")
    finally:
        conn.close()
    
    # Botón para limpiar cache de tareas
    st.markdown('---')
    if st.button('🧹 Cerrar Panel de Tareas'):
        st.session_state.tasks_loaded = False
        st.rerun()

def show_gestion_usuarios():
    """Gestión completa de usuarios del sistema con CRUD optimizada"""
    st.header('👥 Gestión de Usuarios')
    st.info("🔧 Panel completo de administración de usuarios del sistema")
    
    # Inicializar estado de carga de usuarios
    if 'users_management_loaded' not in st.session_state:
        st.session_state.users_management_loaded = False
    
    if not st.session_state.users_management_loaded:
        st.info('👥 **Panel de Gestión de Usuarios Disponible**')
        st.markdown('Administra todos los usuarios del sistema: crear, modificar, eliminar y ver información')
        if st.button('👤 Cargar Gestión de Usuarios', use_container_width=True):
            st.session_state.users_management_loaded = True
            st.rerun()
        return
    
    # Selector de acciones CRUD
    st.subheader("🎯 Seleccionar Acción")
    accion = st.selectbox(
        "¿Qué operación deseas realizar?",
        ["📋 Ver Usuarios", "➕ Crear Usuario", "✏️ Modificar Usuario", "🗑️ Eliminar Usuario"],
        index=0
    )
    
    st.markdown("---")
    
    if accion == "📋 Ver Usuarios":
        mostrar_lista_usuarios()
    elif accion == "➕ Crear Usuario":
        crear_nuevo_usuario()
    elif accion == "✏️ Modificar Usuario":
        modificar_usuario()
    elif accion == "🗑️ Eliminar Usuario":
        eliminar_usuario()
    
    # Botón para cerrar la gestión de usuarios
    st.markdown('---')
    if st.button('🧹 Cerrar Gestión de Usuarios'):
        st.session_state.users_management_loaded = False
        st.rerun()

def mostrar_lista_usuarios():
    """Mostrar lista completa de usuarios con métricas optimizada"""
    st.subheader("📋 Lista de Usuarios Registrados")
    
    try:
        # Get metrics efficiently first
        metrics = get_users_metrics()
        
        # Mostrar métricas
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("👥 Total", metrics['total'])
        col2.metric("✅ Activos", metrics['active'])
        col3.metric("👑 Admin", metrics['by_role'].get('admin', 0))
        col4.metric("💼 Ventas", metrics['by_role'].get('sales', 0))
        col5.metric("🎧 Soporte", metrics['by_role'].get('support', 0))
        
        # Only load full user data when needed
        if metrics['total'] > 0:
            # Load full user data for filtering and display
            conn = get_conn()
            users_df = pd.read_sql_query('''
                SELECT id, username, email, role, full_name, created_at, last_login, is_active
                FROM users
                ORDER BY created_at DESC
            ''', conn)
            conn.close()
            
            # Filtro por rol
            roles_filter = st.multiselect(
                "🎭 Filtrar por rol",
                options=['admin', 'sales', 'support'],
                default=[]
            )
            
            # Filtro por estado
            estado_filter = st.radio(
                "📊 Filtrar por estado",
                options=["Todos", "Solo activos", "Solo inactivos"],
                horizontal=True
            )
            
            # Aplicar filtros
            filtered_df = users_df.copy()
            
            if roles_filter:
                filtered_df = filtered_df[filtered_df['role'].isin(roles_filter)]
            
            if estado_filter == "Solo activos":
                filtered_df = filtered_df[filtered_df['is_active'] == 1]
            elif estado_filter == "Solo inactivos":
                filtered_df = filtered_df[filtered_df['is_active'] == 0]
            
            # Tabla de usuarios con formato mejorado
            st.subheader(f"👤 Usuarios ({len(filtered_df)} encontrados)")
            
            # Formatear la tabla para mejor visualización
            display_df = filtered_df.copy()
            display_df['Estado'] = display_df['is_active'].apply(lambda x: "✅ Activo" if x == 1 else "❌ Inactivo")
            display_df['Rol'] = display_df['role'].apply(lambda x: {"admin": "👑 Admin", "sales": "💼 Ventas", "support": "🎧 Soporte"}.get(x, x))
            display_df = display_df[['username', 'full_name', 'email', 'Rol', 'Estado', 'created_at', 'last_login']]
            display_df.columns = ['Usuario', 'Nombre Completo', 'Email', 'Rol', 'Estado', 'Creado', 'Último Login']
            
            st.dataframe(display_df, use_container_width=True)
            
            # Activar/Desactivar usuarios rápidamente
            st.subheader("⚡ Acciones Rápidas")
            if not filtered_df.empty:
                user_to_manage = st.selectbox("Seleccionar usuario para cambiar estado", 
                                            options=filtered_df['username'].tolist())
                
                if user_to_manage:
                    selected_user = filtered_df[filtered_df['username'] == user_to_manage].iloc[0]
                    current_status = bool(selected_user['is_active'])
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if current_status:
                            if st.button(f"❌ Desactivar {user_to_manage}", use_container_width=True):
                                conn = get_conn()
                                c = conn.cursor()
                                c.execute("UPDATE users SET is_active = 0 WHERE username = ?", (user_to_manage,))
                                conn.commit()
                                conn.close()
                                st.success(f"✅ Usuario {user_to_manage} desactivado")
                                st.rerun()
                        else:
                            if st.button(f"✅ Activar {user_to_manage}", use_container_width=True):
                                conn = get_conn()
                                c = conn.cursor()
                                c.execute("UPDATE users SET is_active = 1 WHERE username = ?", (user_to_manage,))
                                conn.commit()
                                conn.close()
                                st.success(f"✅ Usuario {user_to_manage} activado")
                                st.rerun()
                    
                    with col2:
                        st.info(f"**Estado actual:** {'✅ Activo' if current_status else '❌ Inactivo'}")
                        st.info(f"**Rol:** {selected_user['role'].title()}")
        else:
            st.info("ℹ️ No hay usuarios registrados")
            
    except Exception as e:
        st.error(f"❌ Error al cargar usuarios: {str(e)}")
        if 'conn' in locals():
            conn.close()

def crear_nuevo_usuario():
    """Formulario para crear nuevo usuario"""
    st.subheader("➕ Crear Nuevo Usuario")
    
    with st.form("crear_usuario_form"):
        st.markdown("### 📝 Información del Usuario")
        
        col1, col2 = st.columns(2)
        with col1:
            username = st.text_input("👤 Nombre de Usuario*", placeholder="usuario123")
            email = st.text_input("📧 Email*", placeholder="usuario@empresa.com")
            full_name = st.text_input("📛 Nombre Completo", placeholder="Juan Pérez González")
        
        with col2:
            password = st.text_input("🔐 Contraseña*", type="password", placeholder="Mínimo 6 caracteres")
            password_confirm = st.text_input("🔐 Confirmar Contraseña*", type="password")
            role = st.selectbox("🎭 Rol del Usuario*", 
                               options=["sales", "support", "admin"],
                               format_func=lambda x: {"admin": "👑 Administrador", "sales": "💼 Ventas", "support": "🎧 Soporte"}[x])
        
        st.markdown("### 🔒 Configuración Inicial")
        is_active = st.checkbox("✅ Usuario activo desde creación", value=True)
        
        # Mostrar información del rol seleccionado
        role_info = {
            "admin": "👑 **Administrador**: Acceso completo al sistema, gestión de usuarios, todas las funcionalidades.",
            "sales": "💼 **Ventas**: Acceso al CRM, registro de instituciones, gestión de leads y métricas de ventas.",
            "support": "🎧 **Soporte**: Acceso a consultas, tickets de soporte y tareas de atención al cliente."
        }
        st.info(role_info[role])
        
        submitted = st.form_submit_button("🚀 Crear Usuario", use_container_width=True, type="primary")
        
        if submitted:
            # Validaciones
            errores = []
            
            if not username:
                errores.append("El nombre de usuario es obligatorio")
            elif len(username) < 3:
                errores.append("El nombre de usuario debe tener al menos 3 caracteres")
            
            if not email:
                errores.append("El email es obligatorio")
            elif "@" not in email:
                errores.append("El email debe tener un formato válido")
            
            if not password:
                errores.append("La contraseña es obligatoria")
            elif len(password) < 6:
                errores.append("La contraseña debe tener al menos 6 caracteres")
            
            if password != password_confirm:
                errores.append("Las contraseñas no coinciden")
            
            if errores:
                for error in errores:
                    st.error(f"❌ {error}")
            else:
                # Importar funciones de autenticación
                import hashlib
                import secrets
                
                def hash_password_local(password: str, salt: str = None) -> tuple:
                    if salt is None:
                        salt = secrets.token_hex(32)
                    salted_password = password + salt
                    hashed = hashlib.sha256(salted_password.encode()).hexdigest()
                    return hashed, salt
                
                try:
                    conn = get_conn()
                    c = conn.cursor()
                    
                    # Verificar si el usuario o email ya existe
                    c.execute("SELECT id FROM users WHERE username = ? OR email = ?", (username, email))
                    if c.fetchone():
                        st.error("❌ El nombre de usuario o email ya existe")
                    else:
                        # Crear usuario
                        password_hash, salt = hash_password_local(password)
                        user_id = str(uuid.uuid4())
                        
                        c.execute('''
                        INSERT INTO users (id, username, email, password_hash, salt, role, full_name, created_at, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (user_id, username, email, password_hash, salt, role, full_name, 
                              datetime.now().date(), int(is_active)))
                        
                        conn.commit()
                        conn.close()
                        
                        st.success(f"✅ Usuario '{username}' creado exitosamente")
                        st.balloons()
                        
                        # Mostrar información del usuario creado
                        st.info(f"""
                        **👤 Usuario creado:**
                        - **Nombre:** {username}
                        - **Email:** {email}
                        - **Rol:** {role.title()}
                        - **Estado:** {'Activo' if is_active else 'Inactivo'}
                        """)
                        
                except Exception as e:
                    st.error(f"❌ Error al crear usuario: {str(e)}")
                    if 'conn' in locals():
                        conn.close()

def modificar_usuario():
    """Formulario para modificar usuario existente"""
    st.subheader("✏️ Modificar Usuario")
    
    # Seleccionar usuario a modificar
    conn = get_conn()
    try:
        users_df = pd.read_sql_query('SELECT username, email, role, full_name, is_active FROM users ORDER BY username', conn)
        conn.close()
        
        if users_df.empty:
            st.info("ℹ️ No hay usuarios para modificar")
            return
        
        username_to_edit = st.selectbox("👤 Seleccionar usuario para modificar", 
                                       options=users_df['username'].tolist())
        
        if username_to_edit:
            # Obtener datos actuales del usuario
            user_data = users_df[users_df['username'] == username_to_edit].iloc[0]
            
            st.markdown("---")
            st.markdown("### 📝 Datos Actuales vs Nuevos Datos")
            
            with st.form("modificar_usuario_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**📊 Datos Actuales:**")
                    st.info(f"**Usuario:** {user_data['username']}")
                    st.info(f"**Email:** {user_data['email']}")
                    st.info(f"**Nombre:** {user_data['full_name'] or 'Sin nombre'}")
                    st.info(f"**Rol:** {user_data['role'].title()}")
                    st.info(f"**Estado:** {'✅ Activo' if user_data['is_active'] else '❌ Inactivo'}")
                
                with col2:
                    st.markdown("**✏️ Nuevos Datos:**")
                    new_email = st.text_input("📧 Nuevo Email", value=user_data['email'])
                    new_full_name = st.text_input("📛 Nuevo Nombre Completo", value=user_data['full_name'] or "")
                    new_role = st.selectbox("🎭 Nuevo Rol", 
                                          options=["sales", "support", "admin"],
                                          index=["sales", "support", "admin"].index(user_data['role']),
                                          format_func=lambda x: {"admin": "👑 Administrador", "sales": "💼 Ventas", "support": "🎧 Soporte"}[x])
                    new_is_active = st.checkbox("✅ Usuario activo", value=bool(user_data['is_active']))
                
                st.markdown("### 🔐 Cambiar Contraseña (Opcional)")
                change_password = st.checkbox("🔄 Cambiar contraseña")
                
                new_password = ""
                new_password_confirm = ""
                if change_password:
                    col1, col2 = st.columns(2)
                    with col1:
                        new_password = st.text_input("🔐 Nueva Contraseña", type="password")
                    with col2:
                        new_password_confirm = st.text_input("🔐 Confirmar Nueva Contraseña", type="password")
                
                submitted = st.form_submit_button("💾 Guardar Cambios", use_container_width=True, type="primary")
                
                if submitted:
                    # Validaciones
                    errores = []
                    
                    if not new_email or "@" not in new_email:
                        errores.append("El email debe tener un formato válido")
                    
                    if change_password:
                        if not new_password:
                            errores.append("La nueva contraseña es obligatoria")
                        elif len(new_password) < 6:
                            errores.append("La contraseña debe tener al menos 6 caracteres")
                        elif new_password != new_password_confirm:
                            errores.append("Las contraseñas no coinciden")
                    
                    if errores:
                        for error in errores:
                            st.error(f"❌ {error}")
                    else:
                        try:
                            conn = get_conn()
                            c = conn.cursor()
                            
                            # Verificar si el email ya existe (excluyendo el usuario actual)
                            c.execute("SELECT id FROM users WHERE email = ? AND username != ?", (new_email, username_to_edit))
                            if c.fetchone():
                                st.error("❌ El email ya está en uso por otro usuario")
                            else:
                                # Actualizar datos básicos
                                if change_password:
                                    # Cambiar también la contraseña
                                    import hashlib
                                    import secrets
                                    
                                    def hash_password_local(password: str, salt: str = None) -> tuple:
                                        if salt is None:
                                            salt = secrets.token_hex(32)
                                        salted_password = password + salt
                                        hashed = hashlib.sha256(salted_password.encode()).hexdigest()
                                        return hashed, salt
                                    
                                    password_hash, salt = hash_password_local(new_password)
                                    c.execute('''
                                    UPDATE users SET email=?, full_name=?, role=?, is_active=?, password_hash=?, salt=?
                                    WHERE username=?
                                    ''', (new_email, new_full_name, new_role, int(new_is_active), 
                                         password_hash, salt, username_to_edit))
                                else:
                                    # Solo actualizar datos básicos
                                    c.execute('''
                                    UPDATE users SET email=?, full_name=?, role=?, is_active=?
                                    WHERE username=?
                                    ''', (new_email, new_full_name, new_role, int(new_is_active), username_to_edit))
                                
                                conn.commit()
                                conn.close()
                                
                                st.success(f"✅ Usuario '{username_to_edit}' modificado exitosamente")
                                if change_password:
                                    st.success("✅ Contraseña actualizada correctamente")
                                st.rerun()
                                
                        except Exception as e:
                            st.error(f"❌ Error al modificar usuario: {str(e)}")
                            if 'conn' in locals():
                                conn.close()
                            
    except Exception as e:
        st.error(f"❌ Error al cargar usuarios: {str(e)}")
        if 'conn' in locals():
            conn.close()

def eliminar_usuario():
    """Formulario para eliminar usuario"""
    st.subheader("🗑️ Eliminar Usuario")
    st.warning("⚠️ **Atención:** Esta acción eliminará permanentemente el usuario del sistema.")
    
    # Seleccionar usuario a eliminar
    conn = get_conn()
    try:
        users_df = pd.read_sql_query('SELECT username, email, role, full_name, is_active FROM users WHERE username != "admin" ORDER BY username', conn)
        conn.close()
        
        if users_df.empty:
            st.info("ℹ️ No hay usuarios disponibles para eliminar (excepto admin)")
            return
        
        username_to_delete = st.selectbox("👤 Seleccionar usuario para eliminar", 
                                         options=users_df['username'].tolist(),
                                         help="El usuario 'admin' no aparece en la lista por seguridad")
        
        if username_to_delete:
            # Mostrar información del usuario a eliminar
            user_data = users_df[users_df['username'] == username_to_delete].iloc[0]
            
            st.markdown("---")
            st.markdown("### 👤 Información del Usuario a Eliminar")
            
            col1, col2 = st.columns(2)
            with col1:
                st.error(f"**Usuario:** {user_data['username']}")
                st.error(f"**Email:** {user_data['email']}")
                st.error(f"**Nombre:** {user_data['full_name'] or 'Sin nombre'}")
            
            with col2:
                st.error(f"**Rol:** {user_data['role'].title()}")
                st.error(f"**Estado:** {'✅ Activo' if user_data['is_active'] else '❌ Inactivo'}")
            
            st.markdown("---")
            st.markdown("### ⚠️ Confirmación de Eliminación")
            
            # Doble confirmación
            confirm1 = st.checkbox(f"✅ Confirmo que quiero eliminar el usuario '{username_to_delete}'")
            
            if confirm1:
                confirm2 = st.checkbox("✅ Entiendo que esta acción es irreversible")
                
                if confirm2:
                    tipo_confirmacion = st.text_input(
                        f"📝 Para confirmar, escribe exactamente: **{username_to_delete}**",
                        placeholder=f"Escribe: {username_to_delete}"
                    )
                    
                    if tipo_confirmacion == username_to_delete:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if st.button("🗑️ ELIMINAR USUARIO", use_container_width=True, type="primary"):
                                try:
                                    conn = get_conn()
                                    c = conn.cursor()
                                    c.execute("DELETE FROM users WHERE username = ?", (username_to_delete,))
                                    conn.commit()
                                    conn.close()
                                    
                                    st.success(f"✅ Usuario '{username_to_delete}' eliminado exitosamente")
                                    st.balloons()
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"❌ Error al eliminar usuario: {str(e)}")
                                    if 'conn' in locals():
                                        conn.close()
                        
                        with col2:
                            st.button("❌ Cancelar", use_container_width=True)
                    
                    elif tipo_confirmacion:
                        st.error("❌ El texto no coincide. La eliminación no se puede completar.")
            
    except Exception as e:
        st.error(f"❌ Error al cargar usuarios: {str(e)}")
        if 'conn' in locals():
            conn.close()