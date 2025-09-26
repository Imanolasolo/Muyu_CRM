import streamlit as st
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import altair as alt
import uuid

# ----------------------
# Database utilities
# ----------------------
DB_PATH = "muyu_crm.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    
    # Check if table exists and get its columns
    c.execute("PRAGMA table_info(institutions)")
    existing_columns = [row[1] for row in c.fetchall()]
    
    # institutions table
    c.execute('''
    CREATE TABLE IF NOT EXISTS institutions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        rector_name TEXT NOT NULL,
        rector_email TEXT NOT NULL,
        rector_phone TEXT NOT NULL,
        contraparte_name TEXT NOT NULL,
        contraparte_email TEXT NOT NULL,
        contraparte_phone TEXT NOT NULL,
        website TEXT,
        pais TEXT,
        ciudad TEXT,
        direccion TEXT,
        created_contact DATE,
        last_interaction DATE,
        num_teachers INTEGER,
        num_students INTEGER,
        avg_fee REAL,
        initial_contact_medium TEXT,
        stage TEXT,
        substage TEXT,
        program_proposed TEXT,
        proposal_value REAL,
        contract_start_date DATE,
        contract_end_date DATE,
        observations TEXT,
        assigned_commercial TEXT,
        no_interest_reason TEXT
    )
    ''')
    
    # Add missing columns if table already exists
    if existing_columns:
        new_columns = [
            ('rector_name', 'TEXT'),
            ('rector_email', 'TEXT'),
            ('rector_phone', 'TEXT'),
            ('contraparte_name', 'TEXT'),
            ('contraparte_email', 'TEXT'),
            ('contraparte_phone', 'TEXT'),
            ('contract_start_date', 'DATE'),
            ('contract_end_date', 'DATE')
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    c.execute(f'ALTER TABLE institutions ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass  # Column might already exist
    
    # interactions (history)
    c.execute('''
    CREATE TABLE IF NOT EXISTS interactions (
        id TEXT PRIMARY KEY,
        institution_id TEXT,
        date DATE,
        medium TEXT,
        notes TEXT,
        FOREIGN KEY(institution_id) REFERENCES institutions(id)
    )
    ''')
    # tasks for followups / reminders
    c.execute('''
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        institution_id TEXT,
        title TEXT,
        due_date DATE,
        done INTEGER DEFAULT 0,
        created_at DATE,
        notes TEXT,
        FOREIGN KEY(institution_id) REFERENCES institutions(id)
    )
    ''')
    conn.commit()
    conn.close()

init_db()

# ----------------------
# Helpers
# ----------------------

def now_date():
    return datetime.now().date()

def sql_insert_institution(data: dict):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO institutions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
        data['id'], data['name'], data.get('rector_name'), data.get('rector_email'), data.get('rector_phone'),
        data.get('contraparte_name'), data.get('contraparte_email'), data.get('contraparte_phone'),
        data.get('website'), data.get('pais'), data.get('ciudad'), data.get('direccion'),
        data.get('created_contact'), data.get('last_interaction'),
        data.get('num_teachers'), data.get('num_students'), data.get('avg_fee'), data.get('initial_contact_medium'),
        data.get('stage'), data.get('substage'), data.get('program_proposed'), data.get('proposal_value'), data.get('observations'),
        data.get('assigned_commercial'), data.get('no_interest_reason')
    ))
    conn.commit()
    conn.close()

# Because above INSERT had mismatch, implement safer insert

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


def fetch_institutions_df():
    conn = get_conn()
    df = pd.read_sql_query('SELECT * FROM institutions', conn)
    # Convertir a datetime (fecha y hora) si es posible
    for col in ['created_contact', 'last_interaction']:
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


def create_task(institution_id, title, due_date, notes=None):
    conn = get_conn()
    c = conn.cursor()
    tid = str(uuid.uuid4())
    c.execute('INSERT INTO tasks (id,institution_id,title,due_date,done,created_at,notes) VALUES (?,?,?,?,?,?,?)', (tid,institution_id,title,str(due_date),0,str(now_date()), notes))
    conn.commit()
    conn.close()

# ----------------------
# UI: Sidebar - quick filters + create institution
# ----------------------
st.set_page_config(page_title='MUYU Education CRM', layout='wide')
st.title('CRM Comercial — MUYU Education')

st.sidebar.image('assets/muyu_logo.jpg', use_container_width=True)
menu = st.sidebar.selectbox('Navegación', ['Panel Admin', 'Registrar institución', 'Buscar / Editar', 'Dashboard', 'Tareas & Alertas'])

# Quick filters
st.sidebar.header('Filtros rápidos')
filter_stage = st.sidebar.multiselect('Etapa', options=['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'], default=None)
filter_medium = st.sidebar.multiselect('Medio contacto', options=['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'], default=None)

# Filtros rápidos por país y ciudad
df_all = fetch_institutions_df()
filter_pais = st.sidebar.multiselect('País', options=sorted(df_all['pais'].dropna().unique()), default=None)
filter_ciudad = st.sidebar.multiselect('Ciudad', options=sorted(df_all['ciudad'].dropna().unique()), default=None)

# ----------------------
# Page: Registrar institución
# ----------------------
if menu == 'Registrar institución':
    st.header('Registrar nueva institución')
    with st.expander('Formulario de registro de institución', expanded=False):
        name = st.text_input('Nombre de la institución', max_chars=200)
        
        # CONTACTO section
        st.markdown('**Rector (Obligatorio)**')
        col1, col2, col3 = st.columns([2, 1, 2])
        with col1:
            rector_name = st.text_input('Nombre del Rector*', key='rector_name_reg')
            rector_email = st.text_input('Email del Rector*', key='rector_email_reg')
        with col2:
            rector_country_code = st.selectbox('País', 
                options=['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina'], 
                key='rector_country_reg')
        with col3:
            rector_phone = st.text_input('Celular del Rector* (sin código país)', key='rector_phone_reg', placeholder='987654321')
        
        st.markdown('**Contraparte (Obligatorio)**')
        col1, col2, col3 = st.columns([2, 1, 2])
        with col1:
            contraparte_name = st.text_input('Nombre de la Contraparte*', key='contraparte_name_reg')
            contraparte_email = st.text_input('Email de la Contraparte*', key='contraparte_email_reg')
        with col2:
            contraparte_country_code = st.selectbox('País', 
                options=['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina'], 
                key='contraparte_country_reg')
        with col3:
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
        col1, col2, col3 = st.columns(3)
        with col1:
            stage = st.selectbox('Etapa', ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'])
        with col2:
            substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'])
        with col3:
            program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'])
        col1, col2 = st.columns(2)
        with col1:
            proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f")
        with col2:
            assigned_commercial = st.text_input('Responsable comercial')
        
        # CONTRATO section
        st.markdown('**CONTRATO**')
        col1, col2 = st.columns(2)
        with col1:
            contract_start_date = st.date_input('Inicio de contrato', value=None, key='contract_start_reg')
        with col2:
            contract_end_date = st.date_input('Fin de contrato', value=None, key='contract_end_reg')
        
        observations = st.text_area('Observaciones')
        guardar = st.button('Guardar institución')
        if guardar:
            if not name:
                st.error('El nombre de la institución es obligatorio')
            elif not rector_name or not rector_email or not rector_phone:
                st.error('Todos los campos del Rector son obligatorios')
            elif not contraparte_name or not contraparte_email or not contraparte_phone:
                st.error('Todos los campos de la Contraparte son obligatorios')
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
                st.success('Institución guardada correctamente')

    # Bulk upload expander
    with st.expander('Carga masiva de instituciones', expanded=False):
        st.markdown("""
        **Instrucciones:**
        - Descarga primero el template Excel para conocer el formato exacto
        - Sube un archivo Excel (.xlsx, .xls) o CSV (.csv)
        - El archivo debe contener las siguientes columnas obligatorias:
          - name: Nombre de la institución
          - rector_name: Nombre del Rector
          - rector_email: Email del Rector
          - rector_phone: Teléfono del Rector (con código de país, ej: +593 987654321)
          - contraparte_name: Nombre de la Contraparte
          - contraparte_email: Email de la Contraparte
          - contraparte_phone: Teléfono de la Contraparte (con código de país)
        - Columnas opcionales: website, pais, ciudad, direccion, num_teachers, num_students, avg_fee, initial_contact_medium, stage, substage, program_proposed, proposal_value, observations, assigned_commercial
        """)
        
        # Template download button
        if st.button("📥 Descargar Template Excel", help="Descarga un archivo Excel con el formato exacto y ejemplos"):
            # Create template data
            template_data = {
                'name': ['Universidad Ejemplo 1', 'Colegio San José', 'Instituto Tecnológico ABC'],
                'rector_name': ['Dr. Juan Pérez', 'Lic. María González', 'Ing. Carlos Rodríguez'],
                'rector_email': ['rector@universidad.edu', 'direccion@colegio.edu.ec', 'director@instituto.edu'],
                'rector_phone': ['+593 987654321', '+57 3001234567', '+51 987123456'],
                'contraparte_name': ['Lic. Ana Martínez', 'Prof. Luis Herrera', 'Dra. Patricia Silva'],
                'contraparte_email': ['coordinacion@universidad.edu', 'academico@colegio.edu.ec', 'coordinadora@instituto.edu'],
                'contraparte_phone': ['+593 987654322', '+57 3001234568', '+51 987123457'],
                'website': ['www.universidad.edu', 'www.colegiosanjose.edu.ec', 'www.institutoabc.edu'],
                'pais': ['Ecuador', 'Colombia', 'Perú'],
                'ciudad': ['Quito', 'Bogotá', 'Lima'],
                'direccion': ['Av. Principal 123', 'Calle 45 #12-34', 'Jr. Los Olivos 567'],
                'num_teachers': [150, 25, 80],
                'num_students': [2500, 450, 1200],
                'avg_fee': [350.50, 180.00, 280.75],
                'initial_contact_medium': ['Whatsapp', 'Correo electrónico', 'Reunión virtual'],
                'stage': ['Abierto', 'En Proceso', 'Abierto'],
                'substage': ['Primera reunión', 'Envío propuesta', 'Reunión agendada'],
                'program_proposed': ['Programa Muyu Lab', 'Demo', 'Programa Muyu App'],
                'proposal_value': [15000.00, 0.00, 8500.50],
                'observations': ['Interesados en programa completo', 'Solicitan demo presencial', 'Evalúan presupuesto'],
                'assigned_commercial': ['Juan Comercial', 'María Ventas', 'Carlos Manager'],
                'contract_start_date': ['2024-03-01', '', '2024-04-15'],
                'contract_end_date': ['2025-02-28', '', '2025-04-14']
            }
            
            # Create template DataFrame
            template_df = pd.DataFrame(template_data)
            
            # Create Excel file in memory
            import io
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Sheet with examples
                template_df.to_excel(writer, sheet_name='Ejemplos', index=False)
                
                # Empty template sheet
                empty_df = pd.DataFrame(columns=template_df.columns)
                empty_df.to_excel(writer, sheet_name='Plantilla_Vacia', index=False)
                
                # Instructions sheet
                instructions = pd.DataFrame({
                    'Columna': list(template_df.columns),
                    'Obligatorio': ['SÍ' if col in ['name', 'rector_name', 'rector_email', 'rector_phone', 'contraparte_name', 'contraparte_email', 'contraparte_phone'] else 'NO' for col in template_df.columns],
                    'Descripción': [
                        'Nombre de la institución',
                        'Nombre completo del Rector',
                        'Email del Rector',
                        'Teléfono con código país (+593 987654321)',
                        'Nombre completo de la Contraparte',
                        'Email de la Contraparte', 
                        'Teléfono con código país (+593 987654321)',
                        'Página web (opcional)',
                        'País (Ecuador, Colombia, Perú, México, Chile, Argentina)',
                        'Ciudad donde está ubicada',
                        'Dirección física completa',
                        'Número de docentes (número entero)',
                        'Número de estudiantes (número entero)',
                        'Pensión promedio (número decimal)',
                        'Medio de contacto inicial',
                        'Etapa actual del lead',
                        'Subetapa específica',
                        'Programa que se propuso',
                        'Valor de la propuesta (número decimal)',
                        'Observaciones generales',
                        'Responsable comercial asignado',
                        'Fecha inicio contrato (YYYY-MM-DD)',
                        'Fecha fin contrato (YYYY-MM-DD)'
                    ]
                })
                instructions.to_excel(writer, sheet_name='Instrucciones', index=False)
            
            # Download button
            st.download_button(
                label="📥 Descargar Template",
                data=output.getvalue(),
                file_name="template_carga_masiva_instituciones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        uploaded_file = st.file_uploader(
            "Selecciona el archivo", 
            type=['csv', 'xlsx', 'xls'],
            help="Formatos soportados: CSV, Excel (xlsx, xls)"
        )
        
        if uploaded_file is not None:
            try:
                # Read the file based on its type
                if uploaded_file.name.endswith('.csv'):
                    df_upload = pd.read_csv(uploaded_file)
                else:
                    # For Excel files, try to read the first sheet or a specific sheet
                    try:
                        # First try to read 'Plantilla_Vacia' sheet if it exists (from our template)
                        df_upload = pd.read_excel(uploaded_file, sheet_name='Plantilla_Vacia')
                        if df_upload.empty:
                            # If empty, try the first sheet
                            df_upload = pd.read_excel(uploaded_file, sheet_name=0)
                    except:
                        # If no specific sheet, read the first one
                        df_upload = pd.read_excel(uploaded_file, sheet_name=0)
                
                # Remove completely empty rows
                df_upload = df_upload.dropna(how='all')
                
                if df_upload.empty:
                    st.warning("El archivo está vacío o no contiene datos.")
                else:
                    st.success(f"Archivo cargado exitosamente: {len(df_upload)} filas encontradas")
                    
                    # Show file info
                    st.info(f"**Archivo:** {uploaded_file.name} | **Tamaño:** {uploaded_file.size} bytes")
                    
                    # Show preview of actual uploaded data
                    st.subheader("Vista previa de los datos cargados:")
                    st.dataframe(df_upload.head(10), use_container_width=True)
                    
                    # Show column info
                    st.subheader("Información de columnas:")
                    col_info = pd.DataFrame({
                        'Columna': df_upload.columns.tolist(),
                        'Tipo de datos': [str(df_upload[col].dtype) for col in df_upload.columns],
                        'Valores no nulos': [df_upload[col].notna().sum() for col in df_upload.columns],
                        'Valores únicos': [df_upload[col].nunique() for col in df_upload.columns]
                    })
                    st.dataframe(col_info, use_container_width=True)
                    
                    # Validate required columns - but allow flexibility
                    required_cols = ['name', 'rector_name', 'rector_email', 'rector_phone', 'contraparte_name', 'contraparte_email', 'contraparte_phone']
                    missing_cols = [col for col in required_cols if col not in df_upload.columns]
                    
                    if missing_cols:
                        st.warning(f"⚠️ Faltan las siguientes columnas recomendadas: {', '.join(missing_cols)}")
                        st.info("💡 El sistema creará valores por defecto para los campos faltantes. Podrás editarlos después en el Panel Admin.")
                        
                        # Show what defaults will be used - NO usar expander aquí
                        st.markdown("**Valores por defecto que se asignarán:**")
                        defaults_info = []
                        for col in missing_cols:
                            if 'email' in col:
                                default_val = "sin-email@temp.com"
                            elif 'phone' in col:
                                default_val = "+593 000000000"
                            elif 'name' in col and 'rector' in col:
                                default_val = "Rector Sin Definir"
                            elif 'name' in col and 'contraparte' in col:
                                default_val = "Contraparte Sin Definir"
                            else:
                                default_val = "Sin definir"
                            defaults_info.append({'Campo': col, 'Valor por defecto': default_val})
                        
                        defaults_df = pd.DataFrame(defaults_info)
                        st.dataframe(defaults_df, use_container_width=True)
                    else:
                        st.success("✅ Todas las columnas recomendadas están presentes")
                    
                    # Show data validation summary - but be more flexible
                    st.subheader("Resumen de validación:")
                    
                    # Only require 'name' as truly mandatory
                    truly_required = ['name']
                    missing_truly_required = [col for col in truly_required if col not in df_upload.columns]
                    
                    if missing_truly_required:
                        st.error(f"❌ La columna 'name' es absolutamente obligatoria y no se encuentra en el archivo.")
                        st.stop()
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total de filas", len(df_upload))
                    with col2:
                        # Count rows with at least the name field
                        valid_rows = df_upload.dropna(subset=['name'])
                        st.metric("Filas con nombre", len(valid_rows))
                    with col3:
                        invalid_rows = len(df_upload) - len(valid_rows)
                        st.metric("Filas sin nombre", invalid_rows)
                    
                    if invalid_rows > 0:
                        st.warning("⚠️ Las filas sin nombre de institución serán omitidas")
                        
                        # Show invalid rows - NO usar expander
                        st.markdown("**Filas sin nombre que serán omitidas:**")
                        invalid_df = df_upload[df_upload['name'].isna()]
                        st.dataframe(invalid_df, use_container_width=True)
                    
                    # Additional warnings for missing important data
                    if len(valid_rows) > 0:
                        missing_contacts = 0
                        contact_fields = ['rector_email', 'contraparte_email', 'rector_phone', 'contraparte_phone']
                        available_contact_fields = [field for field in contact_fields if field in df_upload.columns]
                        
                        if available_contact_fields:
                            missing_contacts = len(valid_rows[valid_rows[available_contact_fields].isna().all(axis=1)])
                            if missing_contacts > 0:
                                st.warning(f"⚠️ {missing_contacts} instituciones no tienen información de contacto. Se asignarán valores temporales.")
                    
                    # Process and upload button - always show if we have names
                    if len(valid_rows) > 0:
                        st.info(f"🚀 Listo para procesar {len(valid_rows)} instituciones")
                        
                        if st.button("🚀 Procesar y cargar instituciones", type="primary"):
                            progress_bar = st.progress(0)
                            success_count = 0
                            error_count = 0
                            errors = []
                            warnings = []
                            
                            # Process rows with names
                            valid_df = df_upload.dropna(subset=['name'])
                            
                            for index, row in valid_df.iterrows():
                                try:
                                    # Create institution dict - only name is truly required
                                    inst = {
                                        'id': str(uuid.uuid4()),
                                        'name': str(row['name']).strip(),
                                    }
                                    
                                    # Add required fields with defaults if missing
                                    inst['rector_name'] = str(row.get('rector_name', 'Rector Sin Definir')).strip() if pd.notna(row.get('rector_name')) else 'Rector Sin Definir'
                                    inst['rector_email'] = str(row.get('rector_email', 'rector-sin-email@temp.com')).strip() if pd.notna(row.get('rector_email')) else 'rector-sin-email@temp.com'
                                    inst['rector_phone'] = str(row.get('rector_phone', '+593 000000000')).strip() if pd.notna(row.get('rector_phone')) else '+593 000000000'
                                    inst['contraparte_name'] = str(row.get('contraparte_name', 'Contraparte Sin Definir')).strip() if pd.notna(row.get('contraparte_name')) else 'Contraparte Sin Definir'
                                    inst['contraparte_email'] = str(row.get('contraparte_email', 'contraparte-sin-email@temp.com')).strip() if pd.notna(row.get('contraparte_email')) else 'contraparte-sin-email@temp.com'
                                    inst['contraparte_phone'] = str(row.get('contraparte_phone', '+593 000000000')).strip() if pd.notna(row.get('contraparte_phone')) else '+593 000000000'
                                    
                                    # Track what was filled with defaults
                                    defaults_used = []
                                    if inst['rector_name'] == 'Rector Sin Definir':
                                        defaults_used.append('rector_name')
                                    if 'sin-email@temp.com' in inst['rector_email']:
                                        defaults_used.append('rector_email')
                                    if '000000000' in inst['rector_phone']:
                                        defaults_used.append('rector_phone')
                                    if inst['contraparte_name'] == 'Contraparte Sin Definir':
                                        defaults_used.append('contraparte_name')
                                    if 'sin-email@temp.com' in inst['contraparte_email']:
                                        defaults_used.append('contraparte_email')
                                    if '000000000' in inst['contraparte_phone']:
                                        defaults_used.append('contraparte_phone')
                                    
                                    if defaults_used:
                                        warnings.append(f"Institución '{inst['name']}': Se usaron valores por defecto para: {', '.join(defaults_used)}")
                                    
                                    # Add optional fields with defaults
                                    inst.update({
                                        'website': str(row.get('website', '')).strip() if pd.notna(row.get('website')) else '',
                                        'pais': str(row.get('pais', 'Ecuador')).strip() if pd.notna(row.get('pais')) else 'Ecuador',
                                        'ciudad': str(row.get('ciudad', '')).strip() if pd.notna(row.get('ciudad')) else '',
                                        'direccion': str(row.get('direccion', '')).strip() if pd.notna(row.get('direccion')) else '',
                                        'created_contact': str(now_date()),
                                        'last_interaction': str(now_date()),
                                        'num_teachers': int(float(row.get('num_teachers', 0))) if pd.notna(row.get('num_teachers')) and str(row.get('num_teachers')).replace('.','').replace(',','').isdigit() else 0,
                                        'num_students': int(float(row.get('num_students', 0))) if pd.notna(row.get('num_students')) and str(row.get('num_students')).replace('.','').replace(',','').isdigit() else 0,
                                        'avg_fee': float(str(row.get('avg_fee', 0)).replace(',', '.')) if pd.notna(row.get('avg_fee')) else 0.0,
                                        'initial_contact_medium': str(row.get('initial_contact_medium', 'Whatsapp')).strip() if pd.notna(row.get('initial_contact_medium')) else 'Whatsapp',
                                        'stage': str(row.get('stage', 'Abierto')).strip() if pd.notna(row.get('stage')) else 'Abierto',
                                        'substage': str(row.get('substage', 'Primera reunión')).strip() if pd.notna(row.get('substage')) else 'Primera reunión',
                                        'program_proposed': str(row.get('program_proposed', 'Demo')).strip() if pd.notna(row.get('program_proposed')) else 'Demo',
                                        'proposal_value': float(str(row.get('proposal_value', 0)).replace(',', '.')) if pd.notna(row.get('proposal_value')) else 0.0,
                                        'contract_start_date': str(row.get('contract_start_date')) if pd.notna(row.get('contract_start_date')) else None,
                                        'contract_end_date': str(row.get('contract_end_date')) if pd.notna(row.get('contract_end_date')) else None,
                                        'observations': str(row.get('observations', '')).strip() if pd.notna(row.get('observations')) else '',
                                        'assigned_commercial': str(row.get('assigned_commercial', '')).strip() if pd.notna(row.get('assigned_commercial')) else '',
                                        'no_interest_reason': None
                                    })
                                    
                                    # Save institution
                                    save_institution(inst)
                                    success_count += 1
                                    
                                except Exception as e:
                                    error_count += 1
                                    errors.append(f"Fila {index + 2}: {str(e)}")
                                
                                # Update progress
                                progress = (index + 1) / len(valid_df)
                                progress_bar.progress(progress)
                            

                            # Show results
                            st.success(f"✅ Proceso completado!")
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("✅ Instituciones cargadas", success_count)
                            with col2:
                                st.metric("❌ Errores", error_count)
                            with col3:
                                st.metric("⚠️ Con valores por defecto", len(warnings))
                            

                            if warnings:
                                st.markdown("**Instituciones con valores por defecto:**")
                                for warning in warnings:
                                    st.warning(warning)
                                st.info("💡 Puedes editar estos valores en el Panel Admin después de la carga.")
                            
                            if errors:
                                st.markdown("**Errores detallados:**")
                                for error in errors:
                                    st.error(error)
                            
                            if success_count > 0:
                                st.balloons()
                                st.success("🎉 ¡Carga completada! Ve al Panel Admin para revisar y editar las instituciones cargadas.")
                                st.info("🔄 Recarga la página para ver las nuevas instituciones en el sistema.")
                    else:
                        st.error("❌ No hay filas válidas para procesar. Asegúrate de que al menos la columna 'name' tenga datos.")
                        
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {str(e)}")
                st.info("💡 Verifica que el archivo no esté corrupto y tenga el formato correcto.")

# ----------------------
# Page: Kanban board
# ----------------------
if menu == 'Panel Admin':
    st.header('Panel Admin — Ciclo de vida de leads')
    df = fetch_institutions_df()
    if not df.empty:
        # Apply filters
        if filter_stage:
            df = df[df['stage'].isin(filter_stage)]
        if filter_medium:
            df = df[df['initial_contact_medium'].isin(filter_medium)]
        if filter_pais:
            df = df[df['pais'].isin(filter_pais)]
        if filter_ciudad:
            df = df[df['ciudad'].isin(filter_ciudad)]

        cols = st.columns([1,1,1,1])
        stages = ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado']
        for col, stage_name in zip(cols, stages):
            with col:
                st.subheader(stage_name)
                stage_df = df[df['stage']==stage_name]
                for i,row in stage_df.sort_values('last_interaction',ascending=False).iterrows():
                    with st.expander(f"{row['name']} — {row.get('rector_name', '') or row.get('contraparte_name', '')}"):
                        # Mostrar fecha de última interacción
                        st.markdown(f"**Última interacción:** {row['last_interaction'].date() if not pd.isna(row['last_interaction']) else 'N/A'}")
                        
                        # Campos editables igual que en creación
                        name_edit = st.text_input('Nombre de la institución', value=row['name'], key=f'name_{row["id"]}')
                        
                        # CONTACTO section
                        st.markdown('**CONTACTO**')
                        st.markdown('**Rector (Obligatorio)**')
                        col1, col2, col3 = st.columns([2, 1, 2])
                        with col1:
                            rector_name_edit = st.text_input('Nombre del Rector*', value=row.get('rector_name', ''), key=f'rector_name_{row["id"]}')
                            rector_email_edit = st.text_input('Email del Rector*', value=row.get('rector_email', ''), key=f'rector_email_{row["id"]}')
                        with col2:
                            current_rector_phone = str(row.get('rector_phone', ''))
                            rector_country_options = ['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina']
                            rector_country_index = 0
                            for i, option in enumerate(rector_country_options):
                                if option.split(' ')[1] in current_rector_phone:
                                    rector_country_index = i
                                    break
                            rector_country_code_edit = st.selectbox('País', 
                                options=rector_country_options, 
                                index=rector_country_index,
                                key=f'rector_country_{row["id"]}')
                        with col3:
                            # Extract phone number without country code
                            rector_phone_only = current_rector_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
                            rector_phone_edit = st.text_input('Celular del Rector* (sin código país)', value=rector_phone_only, key=f'rector_phone_{row["id"]}', placeholder='987654321')
                        
                        st.markdown('**Contraparte (Obligatorio)**')
                        col1, col2, col3 = st.columns([2, 1, 2])
                        with col1:
                            contraparte_name_edit = st.text_input('Nombre de la Contraparte*', value=row.get('contraparte_name', ''), key=f'contraparte_name_{row["id"]}')
                            contraparte_email_edit = st.text_input('Email de la Contraparte*', value=row.get('contraparte_email', ''), key=f'contraparte_email_{row["id"]}')
                        with col2:
                            current_contraparte_phone = str(row.get('contraparte_phone', ''))
                            contraparte_country_options = ['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina']
                            contraparte_country_index = 0
                            for i, option in enumerate(contraparte_country_options):
                                if option.split(' ')[1] in current_contraparte_phone:
                                    contraparte_country_index = i
                                    break
                            contraparte_country_code_edit = st.selectbox('País', 
                                options=contraparte_country_options, 
                                index=contraparte_country_index,
                                key=f'contraparte_country_{row["id"]}')
                        with col3:
                            # Extract phone number without country code
                            contraparte_phone_only = current_contraparte_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
                            contraparte_phone_edit = st.text_input('Celular de la Contraparte* (sin código país)', value=contraparte_phone_only, key=f'contraparte_phone_{row["id"]}', placeholder='987654321')
                        
                        website_edit = st.text_input('Página web', value=row['website'], key=f'web_{row["id"]}')
                        pais_edit = st.selectbox('País', options=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'], index=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'].index(row['pais']) if row['pais'] in ['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'] else 0, key=f'pais_{row["id"]}')
                        ciudad_edit = st.text_input('Ciudad', value=row['ciudad'] if 'ciudad' in row else '', key=f'ciudad_{row["id"]}')
                        direccion_edit = st.text_input('Dirección', value=row['direccion'] if 'direccion' in row else '', key=f'direccion_{row["id"]}')
                        col1, col2 = st.columns(2)
                        with col1:
                            created_contact_edit = st.date_input('Fecha de creación de contacto', value=row['created_contact'], key=f'created_contact_{row["id"]}')
                        with col2:
                            last_interaction_edit = st.date_input('Fecha última interacción', value=row['last_interaction'], key=f'last_interaction_{row["id"]}')
                        col1, col2 = st.columns(2)
                        with col1:
                            num_teachers_edit = st.number_input('Número de docentes', min_value=0, step=1, value=int(row['num_teachers']) if not pd.isna(row['num_teachers']) else 0, key=f'teachers_{row["id"]}')
                        with col2:
                            num_students_edit = st.number_input('Número de estudiantes', min_value=0, step=1, value=int(row['num_students']) if not pd.isna(row['num_students']) else 0, key=f'students_{row["id"]}')
                        col1, col2 = st.columns(2)
                        with col1:
                            avg_fee_edit = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f", value=float(row['avg_fee']) if not pd.isna(row['avg_fee']) else 0.0, key=f'fee_{row["id"]}')
                        with col2:
                            initial_contact_medium_edit = st.selectbox('Medio de contacto', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'], index=['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'].index(row['initial_contact_medium']) if row['initial_contact_medium'] in ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'] else 0, key=f'medium_{row["id"]}')
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            stage_edit = st.selectbox('Etapa', ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'], index=['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'].index(row['stage']) if row['stage'] in ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'] else 0, key=f'stage_{row["id"]}')
                        with col2:
                            substage_edit = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'], index=['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'].index(row['substage']) if row['substage'] in ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'] else 0, key=f'substage_{row["id"]}')
                        with col3:
                            program_proposed_edit = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'], index=['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'].index(row['program_proposed']) if row['program_proposed'] in ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'] else 0, key=f'program_{row["id"]}')
                        col1, col2 = st.columns(2)
                        with col1:
                            proposal_value_edit = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f", value=float(row['proposal_value']) if not pd.isna(row['proposal_value']) else 0.0, key=f'proposal_{row["id"]}')
                        with col2:
                            assigned_commercial_edit = st.text_input('Responsable comercial', value=row.get('assigned_commercial') or '', key=f'assign_{row["id"]}')
        
                        # CONTRATO section
                        st.markdown('**CONTRATO**')
                        col1, col2 = st.columns(2)
                        with col1:
                            contract_start_date_edit = st.date_input('Inicio de contrato', value=pd.to_datetime(row.get('contract_start_date')).date() if row.get('contract_start_date') else None, key=f'contract_start_{row["id"]}')
                        with col2:
                            contract_end_date_edit = st.date_input('Fin de contrato', value=pd.to_datetime(row.get('contract_end_date')).date() if row.get('contract_end_date') else None, key=f'contract_end_{row["id"]}')
                        
                        observations_edit = st.text_area('Observaciones', value=row['observations'] or '', key=f'observaciones_{row["id"]}')
                        
                        if st.button('Guardar cambios', key=f'save_{row["id"]}'):
                            from pytz import timezone
                            import datetime
                            def safe_int(val):
                                try:
                                    return int(val)
                                except (ValueError, TypeError):
                                    return 0
                            def safe_float(val):
                                try:
                                    return float(val)
                                except (ValueError, TypeError):
                                    return 0.0
                            tz = timezone('America/Guayaquil')
                            now_ecuador = datetime.datetime.now(tz)
                            
                            # Extract country codes and combine with phone numbers
                            rector_full_phone_edit = rector_country_code_edit.split(' ')[1] + ' ' + rector_phone_edit
                            contraparte_full_phone_edit = contraparte_country_code_edit.split(' ')[1] + ' ' + contraparte_phone_edit
                            
                            conn = get_conn()
                            c = conn.cursor()
                            c.execute('''
                                UPDATE institutions SET name=?, rector_name=?, rector_email=?, rector_phone=?, contraparte_name=?, contraparte_email=?, contraparte_phone=?, website=?, pais=?, ciudad=?, direccion=?, num_teachers=?, num_students=?, avg_fee=?, initial_contact_medium=?, stage=?, substage=?, program_proposed=?, proposal_value=?, contract_start_date=?, contract_end_date=?, observations=?, assigned_commercial=?, no_interest_reason=?, last_interaction=? WHERE id=?
                            ''', (
                                name_edit, rector_name_edit, rector_email_edit, rector_full_phone_edit, contraparte_name_edit, contraparte_email_edit, contraparte_full_phone_edit, website_edit, pais_edit, ciudad_edit, direccion_edit,
                                safe_int(num_teachers_edit), safe_int(num_students_edit), safe_float(avg_fee_edit), initial_contact_medium_edit, stage_edit, substage_edit, program_proposed_edit, safe_float(proposal_value_edit), str(contract_start_date_edit) if contract_start_date_edit else None, str(contract_end_date_edit) if contract_end_date_edit else None, observations_edit, assigned_commercial_edit, None, now_ecuador, row['id']
                            ))
                            conn.commit()
                            conn.close()
                            st.rerun()

                        # Campos para crear tarea (NO usar expander aquí)
                        st.markdown("**Crear nueva tarea para esta institución:**")
                        title = st.text_input('Título de la tarea', key=f'task_title_{row["id"]}')
                        due_date = st.date_input('Fecha de vencimiento', value=now_date(), key=f'task_due_{row["id"]}')
                        notes = st.text_area('Notas de la tarea', key=f'task_notes_{row["id"]}')
                        responsable = st.text_input('Responsable de tarea', key=f'task_responsable_{row["id"]}')
                        responsable_email = st.text_input('Email responsable', key=f'task_responsable_email_{row["id"]}')
                        responsable_whatsapp = st.text_input('Whatsapp responsable', key=f'task_responsable_whatsapp_{row["id"]}')
                        if st.button('Crear tarea', key=f'create_task_{row["id"]}'):
                            # Combina los datos en notes para almacenarlos
                            full_notes = f"{notes}\nResponsable: {responsable}\nEmail: {responsable_email}\nWhatsapp: {responsable_whatsapp}"
                            create_task(row['id'], title, due_date, full_notes)
                            st.success('Tarea creada correctamente')
                            st.rerun()
    else:
        st.info('No hay instituciones registradas aún. Ve a "Registrar institución"')

# ----------------------
# Page: Buscar / Editar
# ----------------------
if menu == 'Buscar / Editar':
    st.header('Buscar o editar instituciones')
    q = st.text_input('Buscar por nombre, rector o email')
    df = fetch_institutions_df()
    if not df.empty:
        if q:
            # Updated search mask to include new contact fields
            mask = (df['name'].str.contains(q, case=False, na=False) | 
                   df['rector_name'].str.contains(q, case=False, na=False) | 
                   df['rector_email'].str.contains(q, case=False, na=False) | 
                   df['contraparte_name'].str.contains(q, case=False, na=False) | 
                   df['contraparte_email'].str.contains(q, case=False, na=False))
            results = df[mask]
        else:
            results = df
        
        # Display all columns in the dataframe
        st.dataframe(results, use_container_width=True)
        
        # Select one to edit por nombre
        name_to_id = dict(zip(results['name'], results['id']))
        sel_name = st.selectbox('Seleccionar institución', options=results['name'].tolist()) if not results.empty else None
        sel = name_to_id.get(sel_name) if sel_name else None
        if sel:
            row = results[results['id']==sel].iloc[0]
            with st.expander('Editar institución', expanded=True):
                name = st.text_input('Nombre de la institución', value=row['name'])
                
                # CONTACTO section
                st.subheader('CONTACTO')
                st.markdown('**Rector (Obligatorio)**')
                col1, col2, col3 = st.columns([2, 1, 2])
                with col1:
                    rector_name = st.text_input('Nombre del Rector*', value=row.get('rector_name', ''))
                    rector_email = st.text_input('Email del Rector*', value=row.get('rector_email', ''))
                with col2:
                    current_rector_phone = str(row.get('rector_phone', ''))
                    rector_country_options = ['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina']
                    rector_country_index = 0
                    for i, option in enumerate(rector_country_options):
                        if option.split(' ')[1] in current_rector_phone:
                            rector_country_index = i
                            break
                    rector_country_code = st.selectbox('País', 
                        options=rector_country_options, 
                        index=rector_country_index,
                        key='rector_country_edit')
                with col3:
                    # Extract phone number without country code
                    rector_phone_only = current_rector_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
                    rector_phone = st.text_input('Celular del Rector* (sin código país)', value=rector_phone_only, placeholder='987654321')
                
                st.markdown('**Contraparte (Obligatorio)**')
                col1, col2, col3 = st.columns([2, 1, 2])
                with col1:
                    contraparte_name = st.text_input('Nombre de la Contraparte*', value=row.get('contraparte_name', ''))
                    contraparte_email = st.text_input('Email de la Contraparte*', value=row.get('contraparte_email', ''))
                with col2:
                    current_contraparte_phone = str(row.get('contraparte_phone', ''))
                    contraparte_country_options = ['🇪🇨 +593 Ecuador', '🇨🇴 +57 Colombia', '🇵🇪 +51 Perú', '🇲🇽 +52 México', '🇨🇱 +56 Chile', '🇦🇷 +54 Argentina']
                    contraparte_country_index = 0
                    for i, option in enumerate(contraparte_country_options):
                        if option.split(' ')[1] in current_contraparte_phone:
                            contraparte_country_index = i
                            break
                    contraparte_country_code = st.selectbox('País', 
                        options=contraparte_country_options, 
                        index=contraparte_country_index,
                        key='contraparte_country_edit')
                with col3:
                    # Extract phone number without country code
                    contraparte_phone_only = current_contraparte_phone.replace('+593', '').replace('+57', '').replace('+51', '').replace('+52', '').replace('+56', '').replace('+54', '').strip()
                    contraparte_phone = st.text_input('Celular de la Contraparte* (sin código país)', value=contraparte_phone_only, placeholder='987654321')
                
                website = st.text_input('Página web', value=row['website'])
                pais = st.selectbox('País', options=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'], index=['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'].index(row['pais']) if row['pais'] in ['Ecuador', 'Colombia', 'Perú', 'México', 'Chile', 'Argentina'] else 0)
                ciudad = st.text_input('Ciudad', value=row['ciudad'] if 'ciudad' in row else '')
                direccion = st.text_input('Dirección', value=row['direccion'] if 'direccion' in row else '')
                col1, col2 = st.columns(2)
                with col1:
                    created_contact = st.date_input('Fecha de creación de contacto', value=row['created_contact'], key=f'created_contact_{row["id"]}')
                with col2:
                    last_interaction = st.date_input('Fecha última interacción', value=row['last_interaction'], key=f'last_interaction_{row["id"]}')
                col1, col2 = st.columns(2)
                with col1:
                    num_teachers = st.number_input('Número de docentes', min_value=0, step=1, value=int(row['num_teachers']) if not pd.isna(row['num_teachers']) and str(row['num_teachers']).isdigit() else 0)
                with col2:
                    num_students = st.number_input('Número de estudiantes', min_value=0, step=1, value=int(row['num_students']) if not pd.isna(row['num_students']) and str(row['num_students']).isdigit() else 0)
                col1, col2 = st.columns(2)
                with col1:
                    avg_fee = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f", value=float(row['avg_fee']) if not pd.isna(row['avg_fee']) and str(row['avg_fee']).replace('.','',1).isdigit() else 0.0)
                with col2:
                    initial_contact_medium = st.selectbox('Medio de contacto', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'], index=['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'].index(row['initial_contact_medium']) if row['initial_contact_medium'] in ['Whatsapp','Correo electrónico','Llamada','Evento','Referido','Reunión virtual','Reunión presencial','Email marketing','Redes Sociales'] else 0)
                col1, col2, col3 = st.columns(3)
                with col1:
                    stage = st.selectbox('Etapa', ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'], index=['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'].index(row['stage']) if row['stage'] in ['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado'] else 0, key=f'stage_{row["id"]}')
                with col2:
                    substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'], index=['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'].index(row['substage']) if row['substage'] in ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado','Factura emitida','Pago recibido'] else 0, key=f'substage_{row["id"]}')
                with col3:
                    program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'], index=['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'].index(row['program_proposed']) if row['program_proposed'] in ['Programa Muyu Lab','Programa Piloto Muyu Lab','Programa Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Programa Piloto Muyu ScaleLab','Demo'] else 0, key=f'program_{row["id"]}')
                col1, col2 = st.columns(2)
                with col1:
                    proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f", value=float(row['proposal_value']) if not pd.isna(row['proposal_value']) and str(row['proposal_value']).replace('.','',1).isdigit() else 0.0, key=f'proposal_{row["id"]}')
                with col2:
                    assigned_commercial = st.text_input('Responsable comercial', value=row.get('assigned_commercial') or '')
                
                # CONTRATO section
                st.markdown('**CONTRATO**')
                col1, col2 = st.columns(2)
                with col1:
                    contract_start_date = st.date_input('Inicio de contrato', value=pd.to_datetime(row.get('contract_start_date')).date() if row.get('contract_start_date') else None, key='contract_start_edit')
                with col2:
                    contract_end_date = st.date_input('Fin de contrato', value=pd.to_datetime(row.get('contract_end_date')).date() if row.get('contract_end_date') else None, key='contract_end_edit')
                
                observations = st.text_area('Observaciones', value=row['observations'] or '')
                guardar = st.button('Guardar cambios')
                eliminar = st.button('Eliminar institución')
                if guardar:
                    if not rector_name or not rector_email or not rector_phone:
                        st.error('Todos los campos del Rector son obligatorios')
                    elif not contraparte_name or not contraparte_email or not contraparte_phone:
                        st.error('Todos los campos de la Contraparte son obligatorios')
                    else:
                        # Extract country codes and combine with phone numbers
                        rector_full_phone = rector_country_code.split(' ')[1] + ' ' + rector_phone
                        contraparte_full_phone = contraparte_country_code.split(' ')[1] + ' ' + contraparte_phone
                        
                        from pytz import timezone
                        import datetime
                        tz = timezone('America/Guayaquil')
                        now_ecuador = datetime.datetime.now(tz)
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute('''
                            UPDATE institutions SET name=?, rector_name=?, rector_email=?, rector_phone=?, contraparte_name=?, contraparte_email=?, contraparte_phone=?, website=?, pais=?, ciudad=?, direccion=?, num_teachers=?, num_students=?, avg_fee=?, initial_contact_medium=?, stage=?, substage=?, program_proposed=?, proposal_value=?, contract_start_date=?, contract_end_date=?, observations=?, assigned_commercial=?, no_interest_reason=?, last_interaction=? WHERE id=?
                        ''', (
                            name, rector_name, rector_email, rector_full_phone, contraparte_name, contraparte_email, contraparte_full_phone, website, pais, ciudad, direccion,
                            int(num_teachers), int(num_students), float(avg_fee), initial_contact_medium, stage, substage, program_proposed, float(proposal_value), str(contract_start_date) if contract_start_date else None, str(contract_end_date) if contract_end_date else None, observations, assigned_commercial, None, now_ecuador, sel
                        ))
                        conn.commit()
                        conn.close()
                        st.success('Cambios guardados')
                if eliminar:
                    conn = get_conn()
                    c = conn.cursor()
                    c.execute('DELETE FROM institutions WHERE id=?', (sel,))
                    conn.commit()
                    conn.close()
                    st.success('Institución eliminada')
                    st.rerun()

# ----------------------
# Page: Dashboard
# ----------------------
if menu == 'Dashboard':
    st.header('Dashboard — Métricas clave')
    df = fetch_institutions_df()
    if df.empty:
        st.info('No hay datos para mostrar')
    else:
        total = len(df)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric('Total de leads', total)
        # % por etapa
        stage_counts = df['stage'].value_counts().reindex(['Abierto','En Proceso','Cerrado / Ganado','Perdido / No interesado']).fillna(0)
        col2.metric('Abiertos', int(stage_counts.get('Abierto',0)))
        col3.metric('En proceso', int(stage_counts.get('En Proceso',0)))
        col4.metric('Cerrados / Ganados', int(stage_counts.get('Cerrado / Ganado',0)))

        # conversion rate: abiertos -> cerrados ganados
        abiertos = stage_counts.get('Abierto',0)
        cerrados = stage_counts.get('Cerrado / Ganado',0)
        conv = (cerrados / abiertos *100) if abiertos>0 else None
        st.write('Tasa conversión (Abierto → Cerrado / Ganado):', f"{conv:.1f}%" if conv is not None else 'N/A')

        # Medio de contacto mas efectivo
        med_counts = df['initial_contact_medium'].value_counts()
        if not med_counts.empty:
            med_df = med_counts.reset_index()
            med_df.columns = ['medium','count']
            chart = alt.Chart(med_df).mark_bar().encode(x='medium', y='count')
            st.altair_chart(chart, use_container_width=True)

        # Tiempo promedio en cada etapa (approx using last_interaction - created_contact)
        df2 = df.copy()
        df2['created_contact'] = pd.to_datetime(df2['created_contact'], errors='coerce')
        df2['last_interaction'] = pd.to_datetime(df2['last_interaction'], errors='coerce')
        df2['days_in_pipeline'] = (df2['last_interaction'] - df2['created_contact']).dt.days
        avg_days_by_stage = df2.groupby('stage')['days_in_pipeline'].mean().reset_index()
        if not avg_days_by_stage.empty:
            chart2 = alt.Chart(avg_days_by_stage).mark_bar().encode(x='stage', y='days_in_pipeline')
            st.altair_chart(chart2, use_container_width=True)

        # Valor potencial acumulado (approx num_teachers * avg_fee)
        df['potential_value'] = df['num_teachers'].fillna(0) * df['avg_fee'].fillna(0)
        total_potential = df['potential_value'].sum()
        st.metric('Valor potencial acumulado (estimado)', f"{total_potential:,.2f}")

# ----------------------
# Page: Tareas & Alertas
# ----------------------
if menu == 'Tareas & Alertas':
    st.header('Tareas y alertas automatizadas')
    conn = get_conn()
    tasks = pd.read_sql_query('''
        SELECT t.id, i.name as institucion, t.title, t.due_date, t.done, t.created_at, t.notes
        FROM tasks t LEFT JOIN institutions i ON t.institution_id = i.id
        ORDER BY t.due_date ASC
    ''', conn, parse_dates=['due_date','created_at'])
    conn.close()
    # Sección para autenticación de admin para borrar tareas
    st.markdown("### Borrar tareas (solo admin)")
    admin_user = st.text_input("Usuario admin", key="admin_user")
    admin_pass = st.text_input("Contraseña admin", type="password", key="admin_pass")
    is_admin = (admin_user == "admin" and admin_pass == "admin123")
    if tasks.empty:
        st.info('No hay tareas registradas')
    else:
        for idx, row in tasks.iterrows():
            col1, col2, col3 = st.columns([8,1,1])
            with col1:
                st.write(f"**{row['title']}** — {row['institucion']} — Vence: {row['due_date'].date() if not pd.isna(row['due_date']) else 'N/A'}")
                st.write(f"Notas: {row['notes'] or ''}")
                st.write(f"Creada: {row['created_at'].date() if not pd.isna(row['created_at']) else 'N/A'}")
            with col2:
                checked = st.checkbox('Done', value=bool(row['done']), key=f"done_{row['id']}")
                if checked != bool(row['done']):
                    conn = get_conn()
                    c = conn.cursor()
                    c.execute('UPDATE tasks SET done=? WHERE id=?', (int(checked), row['id']))
                    conn.commit()
                    conn.close()
                    st.rerun()
            with col3:
                if is_admin:
                    if st.button('Eliminar', key=f'del_task_{row["id"]}'):
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute('DELETE FROM tasks WHERE id=?', (row['id'],))
                        conn.commit()
                        conn.close()
                        st.success('Tarea eliminada')
                        st.rerun()
        # Si prefieres mostrar también el dataframe:
        # st.dataframe(tasks)

    # Alerts: leads without contact > 7 días
    df = fetch_institutions_df()
    if not df.empty:
        df['last_interaction'] = pd.to_datetime(df['last_interaction'], errors='coerce')
        # Convert last_interaction to naive datetime (remove timezone) for comparison
        if pd.api.types.is_datetime64_any_dtype(df['last_interaction']):
            df['last_interaction'] = df['last_interaction'].dt.tz_localize(None)
        stale = df[df['last_interaction'] < (pd.Timestamp.now().tz_localize(None) - pd.Timedelta(days=7))]
        if not stale.empty:
            st.warning('Leads sin contacto > 7 días:')
            for i,row in stale.iterrows():
                st.write(f"{row['name']} — Última interacción: {row['last_interaction'].date() if not pd.isna(row['last_interaction']) else 'N/A'} — Responsable: {row.get('assigned_commercial')}")
                if st.button(f'Marcar tarea de seguimiento ({row["id"]})'):
                    create_task(row['id'], 'Seguimiento - Lead sin contacto >7d', pd.Timestamp.now().date() + timedelta(days=1), notes='Generado desde alerta')
                    st.success('Tarea creada')
                    st.rerun()
        stale = df[df['last_interaction'] < (pd.Timestamp.now() - pd.Timedelta(days=7))]
        if not stale.empty:
            st.warning('Leads sin contacto > 7 días:')
            for i,row in stale.iterrows():
                st.write(f"{row['name']} — Última interacción: {row['last_interaction'].date() if not pd.isna(row['last_interaction']) else 'N/A'} — Responsable: {row.get('assigned_commercial')}")
                if st.button(f'Marcar tarea de seguimiento ({row["id"]})'):
                    create_task(row['id'], 'Seguimiento - Lead sin contacto >7d', pd.Timestamp.now().date() + timedelta(days=1), notes='Generado desde alerta')
                    st.success('Tarea creada')
                    st.rerun()

