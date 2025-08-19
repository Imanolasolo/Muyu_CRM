import streamlit as st
from langchain.llms import OpenAI

def content_manager_dashboard():
    st.header("Asesor Experto en Creación de Contenidos para Manejo de Clientes")

    # Instrucciones en un expander
    with st.expander("Instrucciones de uso del módulo Content Manager", expanded=False):
        st.markdown("""
        1. **Ingresa tu API Key de OpenAI** en la barra lateral.
        2. **Haz preguntas** sobre estrategias, ejemplos o recomendaciones de contenido para clientes.
        3. **Recibe respuestas** de un asesor experto en contenido.
        4. **Copia y usa** los mensajes generados para campañas o comunicación con prospectos.
        5. Si deseas enviar mensajes masivos, utiliza la pestaña de prospectos para obtener los emails.
        """)

    # Solicita la API Key en la barra lateral
    openai_api_key = st.sidebar.text_input("OpenAI API Key", type="password", key="content_manager_api_key")
    st.sidebar.markdown(
        "[¿No tienes clave? Consíguela aquí](https://gptforwork.com/help/knowledge-base/create-openai-api-key)"
    )

    st.markdown(
        "Haz cualquier pregunta sobre cómo crear contenidos efectivos para clientes, estrategias de comunicación, ejemplos de mensajes, campañas, etc. Recibirás una respuesta como si fuera un asesor experto en contenidos."
    )

    user_question = st.text_input("¿Sobre qué necesitas asesoría o contenido?", key="content_manager_input")
    if st.button("Consultar asesor experto", key="content_manager_button") and user_question:
        if not openai_api_key:
            st.error("Por favor, ingresa tu OpenAI API Key en la barra lateral.")
        else:
            prompt = (
                "Eres un asesor experto en creación de contenidos para el manejo de clientes. "
                "Responde de manera clara, profesional y práctica a la siguiente consulta, "
                "proporcionando ejemplos y recomendaciones si es posible.\n\n"
                f"Consulta: {user_question}\n\nRespuesta:"
            )
            # Aumenta el límite de tokens para respuestas más largas
            llm = OpenAI(openai_api_key=openai_api_key, temperature=0.7, max_tokens=1500)
            with st.spinner("Consultando al asesor experto..."):
                try:
                    respuesta = llm(prompt)
                    st.write(respuesta)
                except Exception as e:
                    st.error(f"Error al consultar OpenAI: {e}")

    st.markdown(
        "¿Quieres enviar un mensaje masivo? Copia el contenido generado y usa la pestaña de prospectos para obtener los emails."
    )
