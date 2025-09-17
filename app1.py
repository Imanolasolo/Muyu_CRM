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
    # institutions table
    c.execute('''
    CREATE TABLE IF NOT EXISTS institutions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        rector TEXT,
        rector_position TEXT,
        contact_email TEXT,
        contact_phone TEXT,
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
        observations TEXT,
        assigned_commercial TEXT,
        no_interest_reason TEXT
    )
    ''')
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
        data['id'], data['name'], data.get('rector'), data.get('rector_position'), data.get('contact_email'),
        data.get('contact_phone'), data.get('website'), data.get('pais'), data.get('ciudad'), data.get('direccion'),
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
    INSERT OR REPLACE INTO institutions (id,name,rector,rector_position,contact_email,contact_phone,website,pais,ciudad,direccion,created_contact,last_interaction,num_teachers,num_students,avg_fee,initial_contact_medium,stage,substage,program_proposed,proposal_value,observations,assigned_commercial,no_interest_reason)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        data['id'], data['name'], data.get('rector'), data.get('rector_position'), data.get('contact_email'),
        data.get('contact_phone'), data.get('website'), data.get('pais'), data.get('ciudad'), data.get('direccion'),
        data.get('created_contact'), data.get('last_interaction'),
        data.get('num_teachers'), data.get('num_students'), data.get('avg_fee'), data.get('initial_contact_medium'),
        data.get('stage'), data.get('substage'), data.get('program_proposed'), data.get('proposal_value'), data.get('observations'),
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

menu = st.sidebar.selectbox('Navegación', ['Panel Admin', 'Registrar institución', 'Buscar / Editar', 'Dashboard', 'Tareas & Alertas'])

# Quick filters
st.sidebar.header('Filtros rápidos')
filter_stage = st.sidebar.multiselect('Etapa', options=['Abierto','En proceso','Cerrado','No interesado'], default=None)
filter_medium = st.sidebar.multiselect('Medio contacto', options=['Whatsapp','Correo electrónico','Llamada','Evento','Referido'], default=None)

# Filtros rápidos por país y ciudad
df_all = fetch_institutions_df()
filter_pais = st.sidebar.multiselect('País', options=sorted(df_all['pais'].dropna().unique()), default=None)
filter_ciudad = st.sidebar.multiselect('Ciudad', options=sorted(df_all['ciudad'].dropna().unique()), default=None)

# ----------------------
# Page: Registrar institución
# ----------------------
if menu == 'Registrar institución':
    st.header('Registrar nueva institución')
    with st.expander('Formulario de registro de institución', expanded=True):
        name = st.text_input('Nombre de la institución', max_chars=200)
        col1, col2 = st.columns(2)
        with col1:
            rector = st.text_input('Contraparte, persona de contacto')
        with col2:
            rector_position = st.text_input('Cargo')
        col1, col2 = st.columns(2)
        with col1:
            contact_email = st.text_input('Correo electrónico de contacto')
        with col2:
            contact_phone = st.text_input('Teléfono')
        website = st.text_input('Página web')
        col1, col2, col3 = st.columns(3)
        with col1:
            pais = st.text_input('País')
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
            initial_contact_medium = st.selectbox('Medio de contacto inicial', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'])
        col1, col2, col3 = st.columns(3)
        with col1:
            stage = st.selectbox('Etapa', ['Abierto','En proceso','Cerrado','No interesado'])
        with col2:
            substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'])
        with col3:
            program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'])
        col1, col2 = st.columns(2)
        with col1:
            proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f")
        with col2:
            assigned_commercial = st.text_input('Responsable comercial')
        observations = st.text_area('Observaciones')
        guardar = st.button('Guardar institución')
        if guardar:
            if not name:
                st.error('El nombre de la institución es obligatorio')
            else:
                inst = {
                    'id': str(uuid.uuid4()),
                    'name': name,
                    'rector': rector,
                    'rector_position': rector_position,
                    'contact_email': contact_email,
                    'contact_phone': contact_phone,
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
                    'observations': observations,
                    'no_interest_reason': None,
                    'assigned_commercial': assigned_commercial
                }
                save_institution(inst)
                st.success('Institución guardada correctamente')

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
        stages = ['Abierto','En proceso','Cerrado','No interesado']
        for col, stage_name in zip(cols, stages):
            with col:
                st.subheader(stage_name)
                stage_df = df[df['stage']==stage_name]
                for i,row in stage_df.sort_values('last_interaction',ascending=False).iterrows():
                    with st.expander(f"{row['name']} — {row['rector'] or ''}"):
                        # Mostrar fecha de última interacción
                        st.markdown(f"**Última interacción:** {row['last_interaction'].date() if not pd.isna(row['last_interaction']) else 'N/A'}")
                        # Campos editables igual que en creación
                        name_edit = st.text_input('Nombre de la institución', value=row['name'], key=f'name_{row["id"]}')
                        rector_edit = st.text_input('Contraparte, persona de contacto', value=row['rector'], key=f'rector_{row["id"]}')
                        rector_position_edit = st.text_input('Cargo', value=row['rector_position'], key=f'cargo_{row["id"]}')
                        contact_email_edit = st.text_input('Correo electrónico de contacto', value=row['contact_email'], key=f'email_{row["id"]}')
                        contact_phone_edit = st.text_input('Teléfono', value=row['contact_phone'], key=f'phone_{row["id"]}')
                        website_edit = st.text_input('Página web', value=row['website'], key=f'web_{row["id"]}')
                        pais_edit = st.text_input('País', value=row['pais'] if 'pais' in row else '', key=f'pais_{row["id"]}')
                        ciudad_edit = st.text_input('Ciudad', value=row['ciudad'] if 'ciudad' in row else '', key=f'ciudad_{row["id"]}')
                        direccion_edit = st.text_input('Dirección', value=row['direccion'] if 'direccion' in row else '', key=f'direccion_{row["id"]}')
                        num_teachers_edit = st.number_input('Número de docentes', min_value=0, step=1, value=int(row['num_teachers']) if not pd.isna(row['num_teachers']) else 0, key=f'teachers_{row["id"]}')
                        num_students_edit = st.number_input('Número de estudiantes', min_value=0, step=1, value=int(row['num_students']) if not pd.isna(row['num_students']) else 0, key=f'students_{row["id"]}')
                        avg_fee_edit = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f", value=float(row['avg_fee']) if not pd.isna(row['avg_fee']) else 0.0, key=f'fee_{row["id"]}')
                        initial_contact_medium_edit = st.selectbox('Medio de contacto inicial', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'], index=['Whatsapp','Correo electrónico','Llamada','Evento','Referido'].index(row['initial_contact_medium']) if row['initial_contact_medium'] in ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'] else 0, key=f'medium_{row["id"]}')
                        stage_edit = st.selectbox('Etapa', ['Abierto','En proceso','Cerrado','No interesado'], index=['Abierto','En proceso','Cerrado','No interesado'].index(row['stage']) if row['stage'] in ['Abierto','En proceso','Cerrado','No interesado'] else 0, key=f'stage_{row["id"]}')
                        substage_edit = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'], index=['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'].index(row['substage']) if row['substage'] in ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'] else 0, key=f'substage_{row["id"]}')
                        program_proposed_edit = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'], index=['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'].index(row['program_proposed']) if row['program_proposed'] in ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'] else 0, key=f'program_{row["id"]}')
                        proposal_value_edit = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f", value=float(row['proposal_value']) if not pd.isna(row['proposal_value']) else 0.0, key=f'proposal_{row["id"]}')
                        observaciones_edit = st.text_area('Observaciones', value=row['observations'] or '', key=f'observaciones_{row["id"]}')
                        assign_to = st.text_input('Responsable comercial', value=row.get('assigned_commercial') or '', key=f'assign_{row["id"]}')
                        no_interest_reason_edit = st.text_input('Motivo No interesado', value=row.get('no_interest_reason') or '', key=f'no_interest_{row["id"]}')

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
                            conn = get_conn()
                            c = conn.cursor()
                            c.execute('''
                                UPDATE institutions SET name=?, rector=?, rector_position=?, contact_email=?, contact_phone=?, website=?, pais=?, ciudad=?, direccion=?, num_teachers=?, num_students=?, avg_fee=?, initial_contact_medium=?, stage=?, substage=?, program_proposed=?, proposal_value=?, observations=?, assigned_commercial=?, no_interest_reason=?, last_interaction=? WHERE id=?
                            ''', (
                                name_edit, rector_edit, rector_position_edit, contact_email_edit, contact_phone_edit, website_edit, pais_edit, ciudad_edit, direccion_edit,
                                safe_int(num_teachers_edit), safe_int(num_students_edit), safe_float(avg_fee_edit), initial_contact_medium_edit, stage_edit, substage_edit, program_proposed_edit, safe_float(proposal_value_edit), observaciones_edit, assign_to, no_interest_reason_edit, now_ecuador, row['id']
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
            mask = df['name'].str.contains(q, case=False, na=False) | df['rector'].str.contains(q, case=False, na=False) | df['contact_email'].str.contains(q, case=False, na=False)
            results = df[mask]
        else:
            results = df
        st.dataframe(results)
        # Select one to edit por nombre
        name_to_id = dict(zip(results['name'], results['id']))
        sel_name = st.selectbox('Seleccionar institución', options=results['name'].tolist()) if not results.empty else None
        sel = name_to_id.get(sel_name) if sel_name else None
        if sel:
            row = results[results['id']==sel].iloc[0]
            with st.expander('Editar institución', expanded=True):
                name = st.text_input('Nombre de la institución', value=row['name'])
                col1, col2 = st.columns(2)
                with col1:
                    rector = st.text_input('Contraparte, persona de contacto', value=row['rector'])
                with col2:
                    rector_position = st.text_input('Cargo', value=row['rector_position'])
                contact_email = st.text_input('Correo electrónico de contacto', value=row['contact_email'])
                contact_phone = st.text_input('Teléfono', value=row['contact_phone'])
                website = st.text_input('Página web', value=row['website'])
                pais = st.text_input('País', value=row['pais'] if 'pais' in row else '')
                ciudad = st.text_input('Ciudad', value=row['ciudad'] if 'ciudad' in row else '')
                direccion = st.text_input('Dirección', value=row['direccion'] if 'direccion' in row else '')
                num_teachers = st.number_input('Número de docentes', min_value=0, step=1, value=int(row['num_teachers']) if not pd.isna(row['num_teachers']) and str(row['num_teachers']).isdigit() else 0)
                num_students = st.number_input('Número de estudiantes', min_value=0, step=1, value=int(row['num_students']) if not pd.isna(row['num_students']) and str(row['num_students']).isdigit() else 0)
                avg_fee = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f", value=float(row['avg_fee']) if not pd.isna(row['avg_fee']) and str(row['avg_fee']).replace('.','',1).isdigit() else 0.0)
                initial_contact_medium = st.selectbox('Medio de contacto inicial', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'], index=['Whatsapp','Correo electrónico','Llamada','Evento','Referido'].index(row['initial_contact_medium']) if row['initial_contact_medium'] in ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'] else 0)
                stage = st.selectbox('Etapa', ['Abierto','En proceso','Cerrado','No interesado'], index=['Abierto','En proceso','Cerrado','No interesado'].index(row['stage']) if row['stage'] in ['Abierto','En proceso','Cerrado','No interesado'] else 0)
                substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'], index=['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'].index(row['substage']) if row['substage'] in ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'] else 0)
                program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'], index=['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'].index(row['program_proposed']) if row['program_proposed'] in ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'] else 0)
                proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f", value=float(row['proposal_value']) if not pd.isna(row['proposal_value']) and str(row['proposal_value']).replace('.','',1).isdigit() else 0.0)
                observations = st.text_area('Observaciones', value=row['observations'] or '')
                assigned_commercial = st.text_input('Responsable comercial', value=row.get('assigned_commercial') or '')
                no_interest_reason = st.text_input('Motivo No interesado', value=row.get('no_interest_reason') or '')
                guardar = st.button('Guardar cambios')
                eliminar = st.button('Eliminar institución')
                if guardar:
                    from pytz import timezone
                    import datetime
                    tz = timezone('America/Guayaquil')
                    now_ecuador = datetime.datetime.now(tz)
                    conn = get_conn()
                    c = conn.cursor()
                    c.execute('''
                        UPDATE institutions SET name=?, rector=?, rector_position=?, contact_email=?, contact_phone=?, website=?, pais=?, ciudad=?, direccion=?, num_teachers=?, num_students=?, avg_fee=?, initial_contact_medium=?, stage=?, substage=?, program_proposed=?, proposal_value=?, observations=?, assigned_commercial=?, no_interest_reason=?, last_interaction=? WHERE id=?
                    ''', (
                        name, rector, rector_position, contact_email, contact_phone, website, pais, ciudad, direccion,
                        int(num_teachers), int(num_students), float(avg_fee), initial_contact_medium, stage, substage, program_proposed, float(proposal_value), observations, assigned_commercial, no_interest_reason, now_ecuador, sel
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
        stage_counts = df['stage'].value_counts().reindex(['Abierto','En proceso','Cerrado','No interesado']).fillna(0)
        col2.metric('Abiertos', int(stage_counts.get('Abierto',0)))
        col3.metric('En proceso', int(stage_counts.get('En proceso',0)))
        col4.metric('Cerrados', int(stage_counts.get('Cerrado',0)))

        # conversion rate: abiertos -> cerrados ganados
        abiertos = stage_counts.get('Abierto',0)
        cerrados = stage_counts.get('Cerrado',0)
        conv = (cerrados / abiertos *100) if abiertos>0 else None
        st.write('Tasa conversión (Abierto → Cerrado ganados):', f"{conv:.1f}%" if conv is not None else 'N/A')

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
        stale = df[df['last_interaction'] < (pd.Timestamp.now() - pd.Timedelta(days=7))]
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

