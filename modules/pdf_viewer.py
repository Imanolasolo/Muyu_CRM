import streamlit as st
from PyPDF2 import PdfReader
from langchain.text_splitter import CharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.llms import OpenAI

def get_pdf_text(pdf_file):
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        text += page.extract_text() or ""
    return text

def get_text_chunks(text):
    splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=200)
    return splitter.split_text(text)

def build_vectorstore(chunks, api_key):
    embeddings = OpenAIEmbeddings(openai_api_key=api_key)
    store = FAISS.from_texts(texts=chunks, embedding=embeddings)
    return store

def generate_personalized_response(query, context, api_key):
    llm = OpenAI(openai_api_key=api_key, temperature=0.7)
    prompt = (
        f"Basado en el siguiente fragmento del documento:\n\n{context}\n\n"
        f"Responde de manera clara y concisa a la siguiente pregunta:\n\n{query}"
    )
    return llm(prompt)

def search_pdf(query: str) -> str:
    if "vectorstore" not in st.session_state:
        return "⚠️ No hay PDF procesado todavía."
    docs = st.session_state.vectorstore.similarity_search(query, k=1)
    if not docs or not docs[0].page_content.strip():
        return "No se encontró información relevante en el PDF para tu pregunta."
    context = docs[0].page_content.strip()
    api_key = st.session_state.get("pdf_api_key", None)
    if not api_key:
        return "❌ Ingresa tu API Key de OpenAI."
    return generate_personalized_response(query, context, api_key)

def pdf_viewer_dashboard():
    st.header("PDF Viewer & Chat")
    # Instrucciones en un expander
    with st.expander("Instrucciones de uso del módulo PDF Viewer", expanded=False):
        st.markdown("""
        1. **Carga un archivo PDF** usando el botón correspondiente.
        2. **Procesa el PDF** para habilitar la búsqueda y el chat.
        3. **Haz preguntas** sobre el contenido del PDF en el campo de consulta.
        4. **Recuerda** ingresar tu API Key de OpenAI en la barra lateral.
        5. El sistema buscará el fragmento más relevante y generará una respuesta personalizada.
        """)
    api_key = st.sidebar.text_input("OpenAI API Key", type="password", key="pdf_api_key")
    uploaded_file = st.file_uploader("Elige un archivo PDF", type="pdf", key="pdf_viewer_uploader")
    if st.button("Procesar PDF", key="procesar_pdf_viewer") and uploaded_file:
        with st.spinner("Procesando PDF..."):
            pdf_text = get_pdf_text(uploaded_file)
            text_chunks = get_text_chunks(pdf_text)
            if api_key:
                vectorstore = build_vectorstore(text_chunks, api_key)
                st.session_state.vectorstore = vectorstore
                st.success("✅ PDF procesado y vectorizado.")
            else:
                st.error("❌ Ingresa tu API Key de OpenAI.")
    user_query = st.text_input("¿Qué quieres saber del PDF?", key="pdf_viewer_query")
    if st.button("Preguntar", key="preguntar_pdf_viewer") and user_query:
        with st.spinner("Buscando en el PDF..."):
            answer = search_pdf(user_query)
            st.write(answer)

