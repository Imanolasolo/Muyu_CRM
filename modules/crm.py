import streamlit as st
import pandas as pd
import json
from langchain.llms import OpenAI
import smtplib
from email.mime.text import MIMEText

def send_mass_email(subject, body, recipients):
    # Usa las credenciales de st.secrets
    EMAIL_USER = st.secrets.get("EMAIL_USER")
    EMAIL_PASS = st.secrets.get("EMAIL_PASS")
    if not EMAIL_USER or not EMAIL_PASS:
        st.error("No se encontraron credenciales de email en st.secrets.")
        return False

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        for recipient in recipients:
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = EMAIL_USER
            msg["To"] = recipient
            server.sendmail(EMAIL_USER, recipient, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        st.error(f"Error enviando emails: {e}")
        return False

def crm_dashboard():
    # --- Sidebar: API Key ---
    openai_api_key = st.sidebar.text_input("OpenAI API Key", type="password")
    st.sidebar.markdown(
        "[¿No tienes clave? Consíguela aquí](https://gptforwork.com/help/knowledge-base/create-openai-api-key)"
    )

    st.header("🔎 Chat con tus datos tabulares")
    # Instrucciones en un expander
    with st.expander("Instrucciones de uso del módulo CRM", expanded=False):
        st.markdown("""
        1. **Carga tu archivo de datos**: Usa el botón para cargar archivos CSV, Excel, JSON o Parquet.
        2. **Visualiza la tabla**: Puedes ver la tabla completa en el expander correspondiente.
        3. **Haz preguntas**: Escribe preguntas sobre tus datos o pide enviar mensajes/email a prospectos.
        4. **Envío masivo de emails**: Puedes enviar emails a todos o seleccionar prospectos específicos.
        5. **Selecciona la columna de emails**: Si tu tabla tiene diferentes nombres para la columna de emails, elige la correcta.
        6. **Personaliza tus mensajes**: Completa el asunto y el cuerpo del mensaje antes de enviar.
        7. **Recuerda**: Tu API Key de OpenAI es necesaria para usar el chat inteligente.
        """)

    # --- Carga de archivo tabular ---
    uploaded_table = st.file_uploader(
        "Carga un archivo de datos (csv, xlsx, xls, json, parquet)", 
        type=["csv", "xlsx", "xls", "json", "parquet"]
    )

    def load_table(file):
        try:
            if file.name.endswith(".csv"):
                return pd.read_csv(file)
            elif file.name.endswith(".xlsx") or file.name.endswith(".xls"):
                return pd.read_excel(file)
            elif file.name.endswith(".json"):
                return pd.read_json(file)
            elif file.name.endswith(".parquet"):
                return pd.read_parquet(file)
            else:
                st.error("Formato de archivo no soportado.")
                return None
        except Exception as e:
            st.error(f"❌ Error al procesar el archivo: {e}")
            return None

    if uploaded_table:
        df = load_table(uploaded_table)
        if df is not None:
            st.session_state.tabular_data = df
            st.success(f"✅ Datos cargados ({len(df)} filas, {len(df.columns)} columnas)")
            # Mostrar la tabla dentro de un expander
            with st.expander("Ver tabla completa", expanded=False):
                st.dataframe(df, use_container_width=True, height=500)

    # --- Envío masivo de emails ---
    if "tabular_data" in st.session_state:
        df = st.session_state.tabular_data
        email_columns = [col for col in df.columns if "mail" in col.lower() or "email" in col.lower()]
        if email_columns:
            st.markdown("### Enviar email masivo a prospectos")
            with st.form("mass_email_form"):
                subject = st.text_input("Asunto del email")
                body = st.text_area("Mensaje a enviar")
                selected_col = st.selectbox("Columna de emails", email_columns)
                submit_email = st.form_submit_button("Enviar email masivo")
                if submit_email:
                    recipients = df[selected_col].dropna().unique().tolist()
                    if not subject or not body:
                        st.warning("Debes completar el asunto y el mensaje.")
                    elif not recipients:
                        st.warning("No se encontraron emails en la columna seleccionada.")
                    else:
                        ok = send_mass_email(subject, body, recipients)
                        if ok:
                            st.success(f"Emails enviados a {len(recipients)} prospectos.")
        else:
            st.info("No se detectó ninguna columna de emails en la tabla.")

    # --- Chat con los datos y envío desde chat ---
    if "tabular_data" in st.session_state:
        st.markdown("#### Haz preguntas sobre tus datos o pide enviar mensajes")
        user_input = st.text_input("Tu pregunta:", key="tabular_chat_input")
        send_email_from_chat = False
        chat_email_info = {}

        # Detecta si el usuario pide enviar un email a una persona específica
        import re
        email_columns = [col for col in df.columns if "mail" in col.lower() or "email" in col.lower()]
        name_columns = [col for col in df.columns if "nombre" in col.lower() or "name" in col.lower()]
        match = re.search(r"manda un mail a ([\w\s]+) con el asunto: (.+?) y el mensaje: (.+)", user_input, re.IGNORECASE)
        if match and email_columns and name_columns:
            persona = match.group(1).strip()
            asunto = match.group(2).strip()
            mensaje = match.group(3).strip()
            # Busca la persona en la tabla (ignora mayúsculas/minúsculas)
            df_match = df[df[name_columns[0]].str.lower().str.contains(persona.lower(), na=False)]
            if not df_match.empty:
                destinatarios = df_match[email_columns[0]].dropna().unique().tolist()
                if destinatarios:
                    send_email_from_chat = True
                    chat_email_info = {
                        "persona": persona,
                        "asunto": asunto,
                        "mensaje": mensaje,
                        "destinatarios": destinatarios,
                        "nombre_col": name_columns[0],
                        "email_col": email_columns[0]
                    }
            else:
                st.warning(f"No se encontró a '{persona}' en la columna '{name_columns[0]}'.")

        # NUEVO: Detectar si el usuario pide email masivo a todos los prospectos
        trigger_mass_email_form = False
        # Buscar columnas candidatas a emails de forma más flexible
        def is_email_column(col):
            col_lower = col.lower()
            keywords = [
                "mail", "email", "correo", "e-mail", "e_mail", "e mail", "direccion", "contacto"
            ]
            # Considera columnas que tengan palabras clave y al menos un valor con un '@'
            if any(kw in col_lower for kw in keywords):
                return True
            # También revisa si la mayoría de los valores parecen emails
            sample = df[col].dropna().astype(str).head(20)
            if sample.apply(lambda x: "@" in x and "." in x).sum() > 0:
                return True
            return False

        # Encuentra todas las columnas candidatas a emails
        email_columns = [col for col in df.columns if is_email_column(col)]
        mass_email_keywords = [
            "manda un email masivo", "enviar email masivo", "envía un email masivo",
            "manda un correo masivo", "enviar correo masivo", "envía un correo masivo",
            "manda un mail masivo", "enviar mail masivo", "envía un mail masivo",
            "email masivo a todos", "correo masivo a todos", "mail masivo a todos",
            "email masivo a los prospectos", "correo masivo a los prospectos", "mail masivo a los prospectos"
        ]
        if any(kw in user_input.lower() for kw in mass_email_keywords):
            trigger_mass_email_form = True

        if send_email_from_chat:
            st.markdown(f"### Enviar email a {chat_email_info['persona']} detectado desde el chat")
            with st.form("chat_email_form"):
                subject = st.text_input("Asunto", value=chat_email_info["asunto"], key="chat_subject")
                body = st.text_area("Mensaje", value=chat_email_info["mensaje"], key="chat_body")
                recipients = chat_email_info["destinatarios"]
                st.write(f"Destinatario(s): {', '.join(recipients)}")
                submit_chat_email = st.form_submit_button("Enviar email")
                if submit_chat_email:
                    if not subject or not body:
                        st.warning("Debes completar el asunto y el mensaje.")
                    else:
                        ok = send_mass_email(subject, body, recipients)
                        if ok:
                            st.success(f"Email enviado a {', '.join(recipients)}.")
        elif trigger_mass_email_form:
            st.markdown("### Enviar email masivo a prospectos (detectado desde el chat)")
            if email_columns:
                with st.form("mass_email_form_from_chat"):
                    subject = st.text_input("Asunto del email", key="mass_subject_chat")
                    body = st.text_area("Mensaje a enviar", key="mass_body_chat", value="hola!")
                    selected_col = st.selectbox("Columna de emails", email_columns, key="mass_col_chat")
                    # NUEVO: Selección de prospectos específicos
                    all_prospectos = df[selected_col].dropna().unique().tolist()
                    selected_prospectos = st.multiselect(
                        "Selecciona los prospectos a los que quieres enviar el email (deja vacío para enviar a todos)",
                        all_prospectos,
                        default=all_prospectos
                    )
                    submit_email = st.form_submit_button("Enviar email masivo")
                    if submit_email:
                        recipients = selected_prospectos if selected_prospectos else all_prospectos
                        if not subject or not body:
                            st.warning("Debes completar el asunto y el mensaje.")
                        elif not recipients:
                            st.warning("No se encontraron emails en la columna seleccionada.")
                        else:
                            ok = send_mass_email(subject, body, recipients)
                            if ok:
                                st.success(f"Emails enviados a {len(recipients)} prospectos.")
            else:
                # Si no se detectan columnas candidatas, muestra todas las columnas para que el usuario elija
                st.info("No se detectó ninguna columna de emails en la tabla. Selecciona manualmente la columna que contiene los emails.")
                with st.form("mass_email_form_from_chat_manual"):
                    subject = st.text_input("Asunto del email", key="mass_subject_chat_manual")
                    body = st.text_area("Mensaje a enviar", key="mass_body_chat_manual", value="hola!")
                    selected_col = st.selectbox("Selecciona la columna que contiene los emails", list(df.columns), key="mass_col_chat_manual")
                    # NUEVO: Selección de prospectos específicos
                    all_prospectos = df[selected_col].dropna().unique().tolist()
                    # Filtra solo los que parecen emails
                    all_prospectos = [r for r in all_prospectos if "@" in str(r) and "." in str(r)]
                    selected_prospectos = st.multiselect(
                        "Selecciona los prospectos a los que quieres enviar el email (deja vacío para enviar a todos)",
                        all_prospectos,
                        default=all_prospectos
                    )
                    submit_email = st.form_submit_button("Enviar email masivo")
                    if submit_email:
                        recipients = selected_prospectos if selected_prospectos else all_prospectos
                        if not subject or not body:
                            st.warning("Debes completar el asunto y el mensaje.")
                        elif not recipients:
                            st.warning("No se encontraron emails válidos en la columna seleccionada.")
                        else:
                            ok = send_mass_email(subject, body, recipients)
                            if ok:
                                st.success(f"Emails enviados a {len(recipients)} prospectos.")
        else:
            # NUEVO: Permitir al usuario elegir si quiere enviar toda la tabla al modelo
            use_full_table = False
            df = st.session_state.tabular_data
            max_rows = 1000  # Límite sugerido para advertencia
            if len(df) > max_rows:
                st.warning(
                    f"La tabla tiene {len(df)} filas. Enviar toda la tabla a la IA puede ser lento o costoso. "
                    "Por defecto solo se enviará una muestra. "
                    "Si deseas enviar toda la tabla bajo tu propio riesgo, marca la siguiente casilla."
                )
                use_full_table = st.checkbox("Enviar toda la tabla al modelo (puede ser lento/costoso)", value=False)
            else:
                use_full_table = st.checkbox("Enviar toda la tabla al modelo", value=True)

            # NUEVO: Buscar si el usuario pregunta por una fila y columna específica
            selected_row = None
            selected_col = None
            selected_value = None
            selected_row_info = ""
            # Buscar columnas relevantes
            search_columns = list(df.columns)
            # Buscar si el prompt menciona un valor de columna
            for col in search_columns:
                for val in df[col].dropna().unique():
                    val_str = str(val).strip().lower()
                    if val_str and val_str in user_input.lower():
                        selected_row = df[df[col].astype(str).str.lower() == val_str]
                        selected_col = col
                        selected_value = val
                        if not selected_row.empty:
                            selected_row_info = f"Se ha detectado que tu pregunta se refiere a la fila donde '{col}' = '{val}'. "
                            break
                if selected_row is not None and not selected_row.empty:
                    break

            # Buscar si el usuario pregunta por una columna específica
            col_in_prompt = None
            for col in search_columns:
                if col.lower() in user_input.lower():
                    col_in_prompt = col
                    break

            # NUEVO: Detectar si el usuario pide una lista de una columna (ej: correos electrónicos)
            list_column = None
            list_keywords = [
                "lista de", "correos", "emails", "direcciones de correo", "emails de",
                "correos electrónicos", "columna correo", "columna de correo", "columna de emails",
                "todos los registros de la columna", "todos los valores de la columna"
            ]
            for col in search_columns:
                # Busca si el usuario pide explícitamente todos los registros de una columna
                if (
                    any(kw in user_input.lower() for kw in list_keywords)
                    and (col.lower() in user_input.lower() or "correo" in col.lower() or "mail" in col.lower() or "email" in col.lower())
                ):
                    list_column = col
                    break

            # NUEVO: Detectar si el usuario pregunta por el número de filas de la tabla
            count_rows_keywords = [
                "cuántas filas", "cuantas filas", "número de filas", "numero de filas",
                "cuántos registros", "cuantos registros", "número de registros", "numero de registros",
                "cuántos datos", "cuantos datos", "cuántos hay en la tabla", "cuantos hay en la tabla"
            ]
            ask_count_rows = any(kw in user_input.lower() for kw in count_rows_keywords)

            if st.button("Preguntar", key="tabular_chat_button"):
                if not openai_api_key:
                    st.error("Por favor, ingresa tu OpenAI API Key en la barra lateral.")
                elif not user_input:
                    st.warning("Escribe una pregunta.")
                else:
                    columns = list(df.columns)
                    # Si el usuario pregunta por el número de filas, responde directamente sin usar OpenAI
                    if ask_count_rows:
                        st.info(f"La tabla tiene {len(df)} filas.")
                        return
                    # Si el usuario pide una lista de una columna (como correos)
                    if list_column:
                        values = df[list_column].dropna().tolist()
                        muestra_texto = (
                            f"Has solicitado todos los registros de la columna '{list_column}'. "
                            f"Solo se ha enviado esa columna al modelo para evitar errores de límite de tokens."
                        )
                        context = (
                            f"Tienes una tabla con las siguientes columnas: {columns}.\n"
                            f"A continuación tienes todos los valores de la columna '{list_column}':\n"
                            f"{json.dumps(values, ensure_ascii=False, indent=2, default=str)}\n"
                            f"{muestra_texto}\n"
                            f"Responde la siguiente pregunta del usuario sobre estos datos."
                        )
                    # Si se detecta fila y columna específica, solo enviar ese dato
                    elif selected_row is not None and not selected_row.empty and col_in_prompt:
                        cell_value = selected_row.iloc[0][col_in_prompt]
                        data_to_send = {
                            "columna": col_in_prompt,
                            "valor": cell_value,
                            "toda_la_fila": selected_row.iloc[0].to_dict()
                        }
                        muestra_texto = (
                            f"{selected_row_info}\n"
                            f"Además, se ha detectado que preguntas por la columna '{col_in_prompt}'. "
                            f"Solo se ha enviado ese dato y la fila correspondiente al modelo para evitar errores de límite de tokens."
                        )
                        context = (
                            f"Tienes una tabla con las siguientes columnas: {columns}.\n"
                            f"A continuación tienes el valor solicitado:\n"
                            f"Columna: {col_in_prompt}\nValor: {cell_value}\n"
                            f"Fila completa: {json.dumps(selected_row.iloc[0].to_dict(), ensure_ascii=False, indent=2, default=str)}\n"
                            f"{muestra_texto}\n"
                            f"Responde la siguiente pregunta del usuario sobre este dato."
                        )
                    # Si solo se detecta una fila relevante
                    elif selected_row is not None and not selected_row.empty:
                        data_to_send = selected_row.to_dict(orient="records")
                        muestra_texto = (
                            f"{selected_row_info}\n"
                            f"Solo se ha enviado una muestra de 10 filas. "
                            f"Si necesitas analizar toda la tabla, marca la casilla correspondiente."
                        )
                        context = (
                            f"Tienes una tabla con las siguientes columnas: {columns}.\n"
                            f"Aquí tienes los datos:\n{json.dumps(data_to_send, ensure_ascii=False, indent=2, default=str)}\n"
                            f"{muestra_texto}\n"
                            f"Responde la siguiente pregunta del usuario sobre estos datos."
                        )
                    elif use_full_table:
                        data_to_send = df.to_dict(orient="records")
                        muestra_texto = f"Se ha enviado toda la tabla ({len(df)} filas)."
                        context = (
                            f"Tienes una tabla con las siguientes columnas: {columns}.\n"
                            f"Aquí tienes los datos:\n{json.dumps(data_to_send, ensure_ascii=False, indent=2, default=str)}\n"
                            f"{muestra_texto}\n"
                            f"Responde la siguiente pregunta del usuario sobre estos datos."
                        )
                    else:
                        data_to_send = df.head(10).to_dict(orient="records")
                        muestra_texto = (
                            f"Solo se ha enviado una muestra de 10 filas. "
                            f"Si necesitas analizar toda la tabla, marca la casilla correspondiente."
                        )
                        context = (
                            f"Tienes una tabla con las siguientes columnas: {columns}.\n"
                            f"Aquí tienes los datos:\n{json.dumps(data_to_send, ensure_ascii=False, indent=2, default=str)}\n"
                            f"{muestra_texto}\n"
                            f"Responde la siguiente pregunta del usuario sobre estos datos."
                        )
                    prompt = f"{context}\n\nPregunta: {user_input}\nRespuesta:"
                    llm = OpenAI(openai_api_key=openai_api_key, temperature=0.2)
                    with st.spinner("Consultando a la IA..."):
                        try:
                            response = llm(prompt)
                            st.info(response)
                        except Exception as e:
                            st.error(f"Error al consultar OpenAI: {e}")

            # Si el usuario pide enviar email/mensaje genérico
            if ("enviar email" in user_input.lower() or "enviar mensaje" in user_input.lower()) and not send_email_from_chat:
                st.markdown("### Envío rápido de email detectado desde el chat")
                if email_columns:
                    with st.form("quick_email_form"):
                        subject = st.text_input("Asunto del email (rápido)", key="quick_subject")
                        body = st.text_area("Mensaje a enviar (rápido)", key="quick_body")
                        selected_col = st.selectbox("Columna de emails (rápido)", email_columns, key="quick_col")
                        submit_quick = st.form_submit_button("Enviar email rápido")
                        if submit_quick:
                            recipients = df[selected_col].dropna().unique().tolist()
                            if not subject or not body:
                                st.warning("Debes completar el asunto y el mensaje.")
                            elif not recipients:
                                st.warning("No se encontraron emails en la columna seleccionada.")
                            else:
                                ok = send_mass_email(subject, body, recipients)
                                if ok:
                                    st.success(f"Emails enviados a {len(recipients)} prospectos.")
                else:
                    st.info("No se detectó ninguna columna de emails en la tabla.")
    else:
        st.info("Carga un archivo de datos para comenzar.")