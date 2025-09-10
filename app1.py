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
    c.execute('''INSERT INTO institutions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
        data['id'], data['name'], data.get('rector'), data.get('rector_position'), data.get('contact_email'),
        data.get('contact_phone'), data.get('website'), data.get('created_contact'), data.get('last_interaction'),
        data.get('num_teachers'), data.get('num_students'), data.get('avg_fee'), data.get('initial_contact_medium'),
        data.get('stage'), data.get('substage'), data.get('program_proposed'), data.get('proposal_value'), data.get('observations'),
        # note: table has 18 fields, last field for no_interest_reason - pass None if absent
    ))
    conn.commit()
    conn.close()

# Because above INSERT had mismatch, implement safer insert

def save_institution(data: dict):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
    INSERT OR REPLACE INTO institutions (id,name,rector,rector_position,contact_email,contact_phone,website,created_contact,last_interaction,num_teachers,num_students,avg_fee,initial_contact_medium,stage,substage,program_proposed,proposal_value,observations,no_interest_reason,assigned_commercial)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        data['id'], data['name'], data.get('rector'), data.get('rector_position'), data.get('contact_email'),
        data.get('contact_phone'), data.get('website'), data.get('created_contact'), data.get('last_interaction'),
        data.get('num_teachers'), data.get('num_students'), data.get('avg_fee'), data.get('initial_contact_medium'),
        data.get('stage'), data.get('substage'), data.get('program_proposed'), data.get('proposal_value'), data.get('observations'), data.get('no_interest_reason'), data.get('assigned_commercial')
    ))
    conn.commit()
    conn.close()


def fetch_institutions_df():
    conn = get_conn()
    df = pd.read_sql_query('SELECT * FROM institutions', conn, parse_dates=['created_contact','last_interaction'])
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

menu = st.sidebar.selectbox('Navegación', ['Kanban', 'Registrar institución', 'Buscar / Editar', 'Dashboard', 'Tareas & Alertas'])

# Quick filters
st.sidebar.header('Filtros rápidos')
filter_stage = st.sidebar.multiselect('Etapa', options=['Abierto','En proceso','Cerrado','No interesado'], default=None)
filter_medium = st.sidebar.multiselect('Medio contacto', options=['Whatsapp','Correo electrónico','Llamada','Evento','Referido'], default=None)

# ----------------------
# Page: Registrar institución
# ----------------------
if menu == 'Registrar institución':
    st.header('Registrar nueva institución')
    with st.form('form_new_inst'):
        name = st.text_input('Nombre de la institución', max_chars=200)
        rector = st.text_input('Rector/Directivo principal (nombre completo)')
        rector_position = st.text_input('Cargo')
        contact_email = st.text_input('Correo electrónico de contacto')
        contact_phone = st.text_input('Teléfono')
        website = st.text_input('Página web')
        created_contact = st.date_input('Fecha de creación de contacto', value=now_date())
        last_interaction = st.date_input('Fecha última interacción', value=now_date())
        num_teachers = st.number_input('Número de docentes', min_value=0, step=1)
        num_students = st.number_input('Número de estudiantes', min_value=0, step=1)
        avg_fee = st.number_input('Valor de la pensión promedio', min_value=0.0, format="%.2f")
        initial_contact_medium = st.selectbox('Medio de contacto inicial', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'])
        stage = st.selectbox('Etapa', ['Abierto','En proceso','Cerrado','No interesado'])
        substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'])
        program_proposed = st.selectbox('Programa propuesto', ['Programa Muyu Lab','Programa Piloto Muyu Lab','Muyu App','Programa Piloto Muyu App','Muyu Scale Lab','Demo'])
        proposal_value = st.number_input('Valor propuesta (opcional)', min_value=0.0, format="%.2f")
        observations = st.text_area('Observaciones')
        assigned_commercial = st.text_input('Responsable comercial')
        submitted = st.form_submit_button('Guardar institución')
        if submitted:
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
if menu == 'Kanban':
    st.header('Kanban — Ciclo de vida de leads')
    df = fetch_institutions_df()
    if not df.empty:
        # Apply filters
        if filter_stage:
            df = df[df['stage'].isin(filter_stage)]
        if filter_medium:
            df = df[df['initial_contact_medium'].isin(filter_medium)]

        cols = st.columns([1,1,1,1])
        stages = ['Abierto','En proceso','Cerrado','No interesado']
        for col, stage_name in zip(cols, stages):
            with col:
                st.subheader(stage_name)
                stage_df = df[df['stage']==stage_name]
                for i,row in stage_df.sort_values('last_interaction',ascending=False).iterrows():
                    with st.expander(f"{row['name']} — {row['rector'] or ''}"):
                        st.write('Cargo:', row['rector_position'])
                        st.write('Contacto:', row['contact_email'], ' / ', row['contact_phone'])
                        st.write('Última interacción:', row['last_interaction'])
                        st.write('Programa propuesto:', row['program_proposed'])
                        st.write('Observaciones:', row['observations'])
                        st.write('Asignado a:', row.get('assigned_commercial'))

                        # Actions: move stage, change substage, log interaction, assign
                        new_stage = st.selectbox(f'Mover etapa ({row["id"]})', ['--','Abierto','En proceso','Cerrado','No interesado'], key=f'stage_move_{row["id"]}')
                        new_substage = st.selectbox(f'Subetapa ({row["id"]})', ['--','Primera reunión','Envío propuesta','Negociación','Sin respuesta','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'], key=f'substage_{row["id"]}')
                        assign_to = st.text_input('Asignar responsable comercial', value=row.get('assigned_commercial') or '', key=f'assign_{row["id"]}')
                        # Definir chosen_stage y chosen_substage para ambos contextos
                        chosen_stage = row['stage'] if new_stage == '--' else new_stage
                        chosen_substage = row['substage'] if new_substage == '--' else new_substage
                        if st.button('Guardar cambios', key=f'save_{row["id"]}'):
                            # update
                            conn = get_conn()
                            c = conn.cursor()
                            c.execute('UPDATE institutions SET stage=?, substage=?, assigned_commercial=? WHERE id=?', (chosen_stage, chosen_substage, assign_to, row['id']))
                            conn.commit()
                            conn.close()
                            st.rerun()

                        # Interaction form
                        with st.form(f'interact_{row["id"]}'):
                            medium = st.selectbox('Medio', ['Whatsapp','Correo electrónico','Llamada','Evento','Referido'], key=f'medium_{row["id"]}')
                            notes = st.text_area('Notas', key=f'notes_{row["id"]}')
                            date = st.date_input('Fecha', value=now_date(), key=f'date_{row["id"]}')
                            submitted_i = st.form_submit_button('Registrar interacción')
                            if submitted_i:
                                add_interaction(row['id'], medium, notes, date)
                                st.success('Interacción registrada')
                                # Automations
                                # If moved to Reunión agendada -> create task
                                if (new_substage == 'Reunión agendada') or (row['substage']=='Reunión agendada'):
                                    create_task(row['id'], 'Reunión agendada - confirmar asistencia', date, notes='Auto-generated')
                                # If proposal sent -> follow-up in 48h
                                if (new_substage == 'Envío propuesta') or (chosen_substage=='Envío propuesta'):
                                    follow_up_date = date + timedelta(days=2)
                                    create_task(row['id'], 'Seguimiento - Propuesta enviada', follow_up_date, notes='Auto follow-up 48h')
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
        # Select one to edit
        sel = st.selectbox('Seleccionar institución', options=results['id'].tolist()) if not results.empty else None
        if sel:
            row = results[results['id']==sel].iloc[0]
            with st.form('edit_inst'):
                name = st.text_input('Nombre de la institución', value=row['name'])
                rector = st.text_input('Rector/Directivo principal', value=row['rector'])
                contact_email = st.text_input('Correo', value=row['contact_email'])
                contact_phone = st.text_input('Teléfono', value=row['contact_phone'])
                stage = st.selectbox('Etapa', ['Abierto','En proceso','Cerrado','No interesado'], index=['Abierto','En proceso','Cerrado','No interesado'].index(row['stage']) if row['stage'] in ['Abierto','En proceso','Cerrado','No interesado'] else 0)
                substage = st.selectbox('Subetapa', ['Primera reunión','Envío propuesta','Negociación','Sin respuesta','No interesado','Stand by','Reunión agendada','Revisión contrato','Contrato firmado'], index=0)
                no_interest_reason = st.text_input('Motivo No interesado', value=row.get('no_interest_reason') or '')
                save = st.form_submit_button('Guardar')
                delete = st.form_submit_button('Eliminar institución', type='primary')
                if save:
                    conn = get_conn()
                    c = conn.cursor()
                    c.execute('UPDATE institutions SET name=?, rector=?, contact_email=?, contact_phone=?, stage=?, substage=?, no_interest_reason=? WHERE id=?', (name, rector, contact_email, contact_phone, stage, substage, no_interest_reason or None, sel))
                    conn.commit()
                    conn.close()
                    st.success('Cambios guardados')
                if delete:
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
    # Unir tasks con institutions para mostrar el nombre
    tasks = pd.read_sql_query('''
        SELECT t.id, i.name as institucion, t.title, t.due_date, t.done, t.created_at, t.notes
        FROM tasks t LEFT JOIN institutions i ON t.institution_id = i.id
        ORDER BY t.due_date ASC
    ''', conn, parse_dates=['due_date','created_at'])
    conn.close()
    if tasks.empty:
        st.info('No hay tareas registradas')
    else:
        st.dataframe(tasks)

    # Alerts: leads without contact > 7 days
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

