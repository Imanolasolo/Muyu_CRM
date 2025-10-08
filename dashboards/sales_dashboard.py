"""
Sales Dashboard - Panel específico para usuarios con rol de ventas
Permite ver instituciones asignadas y comunicarse con clientes
"""

import streamlit as st
import sqlite3
from datetime import datetime, timedelta, date
import pandas as pd
import uuid
import urllib.parse
import smtplib
import email.mime.text
import email.mime.multipart

# ----------------------
# Database utilities
# ----------------------
DB_PATH = "muyu_crm.db"

# ----------------------
# Email Configuration (Hardcoded) - Para ventas
# ----------------------
SALES_EMAIL = "ventas@muyu.com"  # Cambia por el email real de ventas
SALES_APP_PASSWORD = "tu_contraseña_app"  # Cambia por la contraseña de aplicación real

def get_conn():
    return sqlite3.connect(DB_PATH)

def now_date():
    return datetime.now().date()

def safe_date_display(date_value):
    """Mostrar fecha de forma segura"""
    try:
        if pd.isna(date_value) or date_value is None:
            return 'N/A'
        if hasattr(date_value, 'date'):
            return date_value.date()
        return str(date_value)
    except:
        return 'N/A'

def safe_date_value(date_value):
    """Convertir fecha para date_input de forma segura"""
    try:
        if pd.isna(date_value) or date_value is None:
            return now_date()
        if isinstance(date_value, str):
            return datetime.strptime(date_value, '%Y-%m-%d').date()
        if hasattr(date_value, 'date'):
            return date_value.date()
        return date_value
    except:
        return now_date()

def get_sales_institutions(username):
    """Obtener instituciones asignadas al usuario de ventas"""
    conn = get_conn()
    
    # Consulta para obtener instituciones donde el usuario es responsable comercial
    query = """
        SELECT * FROM institutions 
        WHERE assigned_commercial = ? 
        ORDER BY stage, last_interaction DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=[username])
        # Convertir fechas de forma segura
        if not df.empty:
            for col in ['last_interaction', 'created_contact', 'contract_start_date', 'contract_end_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
    except Exception as e:
        st.error(f"Error al cargar instituciones: {str(e)}")
        df = pd.DataFrame()
    finally:
        conn.close()
    
    return df

def get_sales_tasks(username):
    """Obtener tareas del usuario de ventas"""
    conn = get_conn()
    
    try:
        # Buscar tareas donde el usuario aparezca en las notas como responsable
        c = conn.cursor()
        c.execute('''
            SELECT t.id, i.name as institucion, t.title, t.due_date, t.done, t.created_at, t.notes
            FROM tasks t 
            LEFT JOIN institutions i ON t.institution_id = i.id
            WHERE t.notes LIKE ? OR i.assigned_commercial = ?
            ORDER BY t.due_date ASC
        ''', (f'%{username}%', username))
        
        task_rows = c.fetchall()
        conn.close()
        
        # Crear DataFrame manualmente
        if task_rows:
            tasks = pd.DataFrame(task_rows, columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
            # Convertir fechas de forma segura
            for col in ['due_date', 'created_at']:
                if col in tasks.columns:
                    tasks[col] = pd.to_datetime(tasks[col], errors='coerce')
        else:
            tasks = pd.DataFrame(columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
            
    except Exception as e:
        st.error(f"Error al cargar tareas: {str(e)}")
        tasks = pd.DataFrame(columns=['id', 'institucion', 'title', 'due_date', 'done', 'created_at', 'notes'])
    
    return tasks

def send_client_email(institution_data, contact_type='rector'):
    """Enviar email al cliente (rector o contraparte)"""
    try:
        # Validar configuración
        if SALES_EMAIL == "ventas@muyu.com" or SALES_APP_PASSWORD == "tu_contraseña_app":
            return False, "⚠️ Configura primero SALES_EMAIL y SALES_APP_PASSWORD en el código"
        
        # Determinar contacto
        if contact_type == 'rector':
            client_name = institution_data.get('rector_name', 'Estimado/a')
            client_email = institution_data.get('rector_email', '')
            client_position = 'Rector/a'
        else:
            client_name = institution_data.get('contraparte_name', 'Estimado/a')
            client_email = institution_data.get('contraparte_email', '')
            client_position = 'Contraparte'
        
        if not client_email:
            return False, f"❌ No hay email registrado para {client_position}"
        
        # Configurar mensaje
        msg = email.mime.multipart.MIMEMultipart()
        msg['From'] = SALES_EMAIL
        msg['To'] = client_email
        msg['Subject'] = f"Seguimiento Comercial - {institution_data['name']}"
        
        # Cuerpo HTML del email
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2E86AB;">🎓 Seguimiento Comercial - Muyu</h2>
            
            <p>Estimado/a <strong>{client_name}</strong>,</p>
            
            <p>Espero que se encuentre muy bien. Me comunico con usted en representación de <strong>Muyu</strong> 
            para hacer seguimiento a nuestra propuesta para <strong>{institution_data['name']}</strong>.</p>
            
            <div style="background-color: #f8f9fa; padding: 20px; border-left: 4px solid #2E86AB; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #2E86AB;">📋 INFORMACIÓN DE LA PROPUESTA</h3>
                <ul style="list-style: none; padding: 0;">
                    <li><strong>🏢 Institución:</strong> {institution_data['name']}</li>
                    <li><strong>🎓 Programa:</strong> {institution_data.get('program_proposed', 'Por definir')}</li>
                    <li><strong>📊 Etapa:</strong> {institution_data.get('stage', 'En proceso')}</li>
                    <li><strong>📅 Último contacto:</strong> {safe_date_display(institution_data.get('last_interaction'))}</li>
                </ul>
            </div>
            
            <p>Me gustaría coordinar una reunión para:</p>
            <ul>
                <li>✅ Revisar los detalles de nuestra propuesta</li>
                <li>✅ Resolver cualquier duda que pueda tener</li>
                <li>✅ Definir los próximos pasos del proceso</li>
            </ul>
            
            <p>¿Cuándo sería un buen momento para una reunión? Estoy disponible para adaptarme a su agenda.</p>
            
            <div style="background-color: #d4edda; padding: 15px; border-left: 4px solid #28a745; margin: 20px 0;">
                <p style="margin: 0;"><strong>📞 Contacto directo:</strong></p>
                <p style="margin: 5px 0;">Email: {SALES_EMAIL}</p>
                <p style="margin: 5px 0;">Nos puede contactar en cualquier momento</p>
            </div>
            
            <p>Quedo atento/a a su respuesta y agradezco su tiempo.</p>
            
            <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
            
            <p style="color: #666; font-size: 14px;">
                Cordiales saludos,<br>
                <strong>Equipo Comercial Muyu</strong><br>
                <em>Transformando la educación a través de la tecnología</em>
            </p>
        </body>
        </html>
        """
        
        # Adjuntar cuerpo
        msg.attach(email.mime.text.MIMEText(html_body, 'html', 'utf-8'))
        
        # Enviar
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SALES_EMAIL, SALES_APP_PASSWORD)
        server.sendmail(SALES_EMAIL, client_email, msg.as_string())
        server.quit()
        
        return True, f"✅ Email enviado a {client_name} ({client_email})"
        
    except smtplib.SMTPAuthenticationError:
        return False, "❌ Error de autenticación. Verifica email y contraseña"
    except Exception as e:
        return False, f"❌ Error al enviar: {str(e)}"

def create_client_whatsapp(institution_data, contact_type='rector'):
    """Crear mensaje de WhatsApp para cliente"""
    try:
        # Determinar contacto
        if contact_type == 'rector':
            client_name = institution_data.get('rector_name', 'Estimado/a')
            phone_number = institution_data.get('rector_phone', '')
        else:
            client_name = institution_data.get('contraparte_name', 'Estimado/a')
            phone_number = institution_data.get('contraparte_phone', '')
        
        if not phone_number:
            return False, "❌ No hay número de teléfono registrado"
        
        # Crear mensaje
        message = f"""
🎓 *Seguimiento Comercial - Muyu*

Hola {client_name}, espero que se encuentre muy bien.

Me comunico para hacer seguimiento a nuestra propuesta para *{institution_data['name']}*.

📋 *INFORMACIÓN DE LA PROPUESTA:*
• *Institución:* {institution_data['name']}
• *Programa:* {institution_data.get('program_proposed', 'Por definir')}
• *Etapa actual:* {institution_data.get('stage', 'En proceso')}

¿Cuándo podríamos coordinar una reunión para revisar los detalles y resolver cualquier duda?

Estoy disponible para adaptarme a su agenda.

Saludos cordiales,
*Equipo Comercial Muyu* 🚀
        """.strip()
        
        # Crear URL WhatsApp
        clean_number = ''.join(filter(str.isdigit, phone_number))
        encoded_message = urllib.parse.quote(message)
        whatsapp_url = f"https://wa.me/{clean_number}?text={encoded_message}"
        
        return True, whatsapp_url
        
    except Exception as e:
        return False, f"❌ Error: {str(e)}"

def create_task(institution_id, title, due_date, notes=''):
    """Crear una nueva tarea"""
    try:
        conn = get_conn()
        c = conn.cursor()
        
        task_id = str(uuid.uuid4())
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
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
        return False

def render_sales_dashboard(current_user_data=None):
    """Dashboard principal para usuarios de ventas"""
    
    # Validar que se recibió la información del usuario
    if not current_user_data:
        st.error("Error de autenticación. Por favor, inicia sesión nuevamente.")
        return
    
    # Extraer información del usuario
    current_user = current_user_data['username']
    full_name = current_user_data.get('full_name', current_user)
    
    st.markdown("# 💼 Panel de Ventas")
    st.markdown(f"**Bienvenido/a {full_name}** - Panel personalizado para gestión comercial")
    
    # Información sobre el panel
    st.info("🎯 **Panel de Ventas**: Aquí puedes ver las instituciones que tienes asignadas como responsable comercial y comunicarte directamente con tus clientes")
    
    # Crear tabs
    tab1, tab2, tab3 = st.tabs([
        "🏢 Mis Instituciones", 
        "📋 Mis Tareas",
        "📊 Mi Dashboard"
    ])
    
    with tab1:
        show_my_institutions(current_user)
    
    with tab2:
        show_my_tasks(current_user)
    
    with tab3:
        show_my_metrics(current_user)

def show_my_institutions(username):
    """Mostrar instituciones asignadas al usuario de ventas"""
    st.header('🏢 Mis Instituciones Asignadas')
    
    # Cargar instituciones
    df = get_sales_institutions(username)
    
    if df.empty:
        st.info('ℹ️ No tienes instituciones asignadas como responsable comercial')
        return
    
    # Mostrar métricas rápidas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = len(df)
        st.metric("Total Asignadas", total)
    
    with col2:
        en_proceso = len(df[df['stage'] == 'En Proceso'])
        st.metric("En Proceso", en_proceso)
    
    with col3:
        ganadas = len(df[df['stage'] == 'Ganado'])
        st.metric("Ganadas", ganadas)
    
    with col4:
        cola = len(df[df['stage'] == 'En cola'])
        st.metric("En Cola", cola)
    
    st.markdown("---")
    
    # Mostrar instituciones por etapa
    stages = ['En cola', 'En Proceso', 'Ganado', 'No interesado']
    cols = st.columns(len(stages))
    
    for col, stage in zip(cols, stages):
        with col:
            stage_df = df[df['stage'] == stage]
            st.subheader(f"{stage} ({len(stage_df)})")
            
            if stage_df.empty:
                st.info(f"Sin instituciones en {stage}")
            else:
                for idx, row in stage_df.iterrows():
                    with st.expander(f"🏢 {row['name']}", expanded=False):
                        
                        # Información básica
                        st.write(f"**📍 Ubicación:** {row.get('pais', 'N/A')}, {row.get('ciudad', 'N/A')}")
                        st.write(f"**📅 Último contacto:** {safe_date_display(row['last_interaction'])}")
                        st.write(f"**🎓 Programa:** {row.get('program_proposed', 'N/A')}")
                        st.write(f"**💰 Valor propuesta:** ${row.get('proposal_value', 0):,.2f}")
                        
                        # Contactos
                        st.markdown("**👥 Contactos:**")
                        st.write(f"📧 **Rector:** {row.get('rector_name', 'N/A')} - {row.get('rector_email', 'N/A')}")
                        st.write(f"🤝 **Contraparte:** {row.get('contraparte_name', 'N/A')} - {row.get('contraparte_email', 'N/A')}")
                        
                        # Observaciones
                        if row.get('observations'):
                            st.write(f"**📝 Observaciones:** {row['observations']}")
                        
                        st.markdown("---")
                        
                        # Sección de comunicación con clientes
                        st.markdown("**📞 Comunicación con Cliente:**")
                        
                        # Verificar configuración de email
                        if SALES_EMAIL == "ventas@muyu.com":
                            st.warning("⚠️ Configura SALES_EMAIL y SALES_APP_PASSWORD para enviar emails")
                        
                        col1, col2 = st.columns(2)
                        
                        # Comunicación con Rector
                        with col1:
                            st.markdown("**👨‍💼 Rector:**")
                            
                            if st.button(f'📧 Email Rector', key=f'email_rector_{row["id"]}', use_container_width=True):
                                with st.spinner('📧 Enviando email...'):
                                    success, message = send_client_email(row, 'rector')
                                    if success:
                                        st.success(message)
                                        st.balloons()
                                    else:
                                        st.error(message)
                            
                            if st.button(f'💬 WhatsApp Rector', key=f'wa_rector_{row["id"]}', use_container_width=True):
                                success, result = create_client_whatsapp(row, 'rector')
                                if success:
                                    st.success("✅ Abriendo WhatsApp...")
                                    st.markdown(f"[🔗 Abrir WhatsApp]({result})")
                                    st.components.v1.html(f'<script>window.open("{result}", "_blank");</script>', height=0)
                                else:
                                    st.error(result)
                        
                        # Comunicación con Contraparte
                        with col2:
                            st.markdown("**🤝 Contraparte:**")
                            
                            if st.button(f'📧 Email Contraparte', key=f'email_contra_{row["id"]}', use_container_width=True):
                                with st.spinner('📧 Enviando email...'):
                                    success, message = send_client_email(row, 'contraparte')
                                    if success:
                                        st.success(message)
                                        st.balloons()
                                    else:
                                        st.error(message)
                            
                            if st.button(f'💬 WhatsApp Contraparte', key=f'wa_contra_{row["id"]}', use_container_width=True):
                                success, result = create_client_whatsapp(row, 'contraparte')
                                if success:
                                    st.success("✅ Abriendo WhatsApp...")
                                    st.markdown(f"[🔗 Abrir WhatsApp]({result})")
                                    st.components.v1.html(f'<script>window.open("{result}", "_blank");</script>', height=0)
                                else:
                                    st.error(result)
                        
                        # Crear tarea de seguimiento
                        st.markdown("**➕ Crear Tarea de Seguimiento:**")
                        with st.form(key=f"task_form_{row['id']}"):
                            task_title = st.text_input('Título de la tarea', 
                                                     value=f"Seguimiento comercial - {row['name']}")
                            task_date = st.date_input('Fecha de vencimiento', 
                                                    value=now_date() + timedelta(days=3))
                            task_notes = st.text_area('Notas', 
                                                     placeholder='Ej: Llamar para agendar reunión...')
                            
                            if st.form_submit_button('➕ Crear Tarea'):
                                if task_title:
                                    notes_with_user = f"{task_notes}\nResponsable: {username}"
                                    if create_task(row['id'], task_title, task_date, notes_with_user):
                                        st.success('✅ Tarea creada correctamente')
                                        st.rerun()
                                else:
                                    st.error("❌ El título es obligatorio")

def show_my_tasks(username):
    """Mostrar tareas del usuario de ventas"""
    st.header('📋 Mis Tareas')
    
    # Cargar tareas
    tasks = get_sales_tasks(username)
    
    if tasks.empty:
        st.info('ℹ️ No tienes tareas asignadas')
        return
    
    # Mostrar métricas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_tasks = len(tasks)
        st.metric("Total Tareas", total_tasks)
    
    with col2:
        completed = len(tasks[tasks['done'] == 1])
        st.metric("Completadas", completed)
    
    with col3:
        pending = len(tasks[tasks['done'] == 0])
        st.metric("Pendientes", pending)
    
    st.markdown("---")
    
    # Mostrar tareas
    for idx, row in tasks.iterrows():
        with st.expander(f"{'✅' if row['done'] else '⏳'} {row['title']} - {row['institucion']}", expanded=False):
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**📅 Vence:** {safe_date_display(row['due_date'])}")
                st.write(f"**🏢 Institución:** {row['institucion']}")
                st.write(f"**📝 Notas:** {row['notes'] or 'Sin notas'}")
                st.write(f"**📅 Creada:** {safe_date_display(row['created_at'])}")
            
            with col2:
                # Marcar como completada
                checked = st.checkbox('✅ Completada', 
                                    value=bool(row['done']), 
                                    key=f"sales_done_{row['id']}")
                
                if checked != bool(row['done']):
                    conn = get_conn()
                    c = conn.cursor()
                    c.execute('UPDATE tasks SET done=? WHERE id=?', (int(checked), row['id']))
                    conn.commit()
                    conn.close()
                    st.rerun()

def show_my_metrics(username):
    """Mostrar métricas del usuario de ventas"""
    st.header('📊 Mi Dashboard de Rendimiento')
    
    # Cargar datos
    df = get_sales_institutions(username)
    tasks = get_sales_tasks(username)
    
    if df.empty:
        st.info('ℹ️ No tienes datos para mostrar métricas')
        return
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_institutions = len(df)
        st.metric("🏢 Instituciones", total_institutions)
    
    with col2:
        total_value = df['proposal_value'].fillna(0).sum()
        st.metric("💰 Valor Total", f"${total_value:,.0f}")
    
    with col3:
        won_count = len(df[df['stage'] == 'Ganado'])
        conversion_rate = (won_count / total_institutions * 100) if total_institutions > 0 else 0
        st.metric("📈 Tasa Conversión", f"{conversion_rate:.1f}%")
    
    with col4:
        completed_tasks = len(tasks[tasks['done'] == 1]) if not tasks.empty else 0
        total_tasks = len(tasks) if not tasks.empty else 0
        task_completion = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
        st.metric("✅ Tareas Completadas", f"{task_completion:.1f}%")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Distribución por Etapa")
        if not df.empty:
            stage_counts = df['stage'].value_counts()
            st.bar_chart(stage_counts)
    
    with col2:
        st.subheader("🌍 Distribución por País")
        if not df.empty:
            country_counts = df['pais'].value_counts()
            st.bar_chart(country_counts)
    
    # Instituciones próximas a vencer sin contacto
    st.subheader("⚠️ Instituciones que Requieren Seguimiento")
    
    if not df.empty:
        # Filtrar instituciones sin contacto reciente (más de 7 días)
        # Ensure both datetime objects are timezone-naive for proper subtraction
        current_time = datetime.now()
        df['last_interaction_dt'] = pd.to_datetime(df['last_interaction']).dt.tz_localize(None)
        df['days_since_contact'] = (current_time - df['last_interaction_dt']).dt.days
        stale_institutions = df[df['days_since_contact'] > 7]
        
        if not stale_institutions.empty:
            st.warning(f"⚠️ {len(stale_institutions)} instituciones sin contacto > 7 días")
            
            for idx, row in stale_institutions.iterrows():
                st.write(f"🏢 **{row['name']}** - Último contacto: {safe_date_display(row['last_interaction'])} ({row['days_since_contact']} días)")
        else:
            st.success("✅ Todas las instituciones tienen contacto reciente")

# Función principal exportada para app1.py
def show_sales_dashboard():
    """Función de compatibilidad - redirige a render_sales_dashboard"""
    st.error("Error: Esta función necesita información del usuario para funcionar correctamente.")
    st.info("Por favor, contacta al administrador del sistema.")