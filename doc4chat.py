import streamlit as st
st.set_page_config(page_title="Doc4Chat", page_icon="🦜")
import fitz  # PyMuPDF library
from langchain.llms import OpenAI

# --- Traducción simple (diccionario) ---
TRANSLATIONS = {
    "en": {
        "title": "🦜 Doc4Chat: Interact with Your PDFs in a Conversational Way",
        "subtitle": "Load your PDF, ask questions, and receive answers directly from the document.",
        "sidebar_api": "OpenAI API Key",
        "sidebar_get_key": "Do you want to get your OpenAI API key?",
        "sidebar_get_key_link": "[Get a free OpenAI API key](https://gptforwork.com/help/knowledge-base/create-openai-api-key)",
        "sidebar_instructions": "Instructions",
        "instructions": """
1. **Input your OpenAI API key**: Enter your OpenAI API key in the provided field.
2. **Upload your knowledge base PDF**: Upload the PDF file containing the information you want to chat about.
3. **Chat with Doc4Chat**: Type your question in the input box and click 'Ask' to chat with Doc4Chat based on your uploaded knowledge base.
""",
        "sidebar_powered": (
            '<a href="https://wa.me/593993513082?text=I%20am%20interested%20in%20AI%20consultancy" target="_blank">'
            '💬 Contact us on WhatsApp for AI consultancy</a>'
        ),
        "upload_label": "Upload PDF file about the topic",
        "welcome": "Welcome to Doc4Chat! Feel free to ask any questions about the topic.",
        "you": "You:",
        "ask": "Ask",
        "response_prefix": "Doc4Chat:",
        "error_api": "Please enter your OpenAI API key in the sidebar."
    },
    "es": {
        "title": "🦜 Doc4Chat: Interactúa con tus PDFs de forma conversacional",
        "subtitle": "Carga tu PDF, haz preguntas y recibe respuestas directamente del documento.",
        "sidebar_api": "Clave API de OpenAI",
        "sidebar_get_key": "¿Quieres obtener tu clave API de OpenAI?",
        "sidebar_get_key_link": "[Obtén una clave API gratuita de OpenAI](https://gptforwork.com/help/knowledge-base/create-openai-api-key)",
        "sidebar_instructions": "Instrucciones",
        "instructions": """
1. **Introduce tu clave API de OpenAI**: Ingresa tu clave API de OpenAI en el campo proporcionado.
2. **Sube tu PDF base de conocimiento**: Sube el archivo PDF con la información sobre la que quieres chatear.
3. **Chatea con Doc4Chat**: Escribe tu pregunta en la caja de texto y haz clic en 'Preguntar' para conversar con Doc4Chat usando tu PDF.
""",
        "sidebar_powered": (
            '<a href="https://wa.me/593993513082?text=estoy%20interesado%20en%20consultoria%20IA" target="_blank">'
            '💬 Contáctanos por WhatsApp para consultoría IA</a>'
        ),
        "upload_label": "Sube un archivo PDF sobre el tema",
        "welcome": "¡Bienvenido a Doc4Chat! Puedes hacer cualquier pregunta sobre el tema.",
        "you": "Tú:",
        "ask": "Preguntar",
        "response_prefix": "Doc4Chat:",
        "error_api": "Por favor, introduce tu clave API de OpenAI en la barra lateral."
    }
}

# --- Estado de idioma ---
if "lang" not in st.session_state:
    st.session_state.lang = "en"

# --- Selector de idioma ---
col_lang1, col_lang2 = st.columns([1, 1])
with col_lang1:
    if st.button("English"):
        st.session_state.lang = "en"
with col_lang2:
    if st.button("Español"):
        st.session_state.lang = "es"

T = TRANSLATIONS[st.session_state.lang]

# Function to extract text from PDF file
def extract_text_from_pdf(file):
    pdf_text = ""
    try:
        with fitz.open(stream=file.read(), filetype="pdf") as doc:
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                pdf_text += page.get_text()
    except Exception as e:
        st.error(f"Error: Unable to read the PDF file. {e}")
    return pdf_text

# Function to summarize a chunk of text
def summarize_chunk(chunk, openai_api_key):
    llm = OpenAI(openai_api_key=openai_api_key, temperature=0.7, max_tokens=1500)
    prompt = f"Please summarize the following text:\n\n{chunk}"
    summary = llm(prompt)
    return summary

# Function to divide text into chunks of approximately 2000 tokens
def chunk_text(text, chunk_size=2000):
    words = text.split()
    chunks = [' '.join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]
    return chunks

# Function to generate response using ChatGPT based on PDF content
def generate_response(input_text, pdf_text, openai_api_key):
    chunks = chunk_text(pdf_text)
    llm = OpenAI(openai_api_key=openai_api_key, temperature=0.7, max_tokens=1500)
    response = ""
    for chunk in chunks:
        prompt = f"Based on the following document:\n\n{chunk}\n\nAnswer the following question:\n\n{input_text}"
        response_chunk = llm(prompt)
        response += response_chunk + "\n"
        if input_text.lower() in response_chunk.lower():
            break
    return response

# Sidebar - Input OpenAI API key
openai_api_key = st.sidebar.text_input(T["sidebar_api"], type='password')
st.sidebar.write(T["sidebar_get_key"])
st.sidebar.markdown(T["sidebar_get_key_link"])

st.markdown(
        f"""
        <style>
        .stApp {{
            background: url({'data:image/jpeg;base64,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'}) no-repeat center center fixed;
            background-size: {'cover'};
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

st.title(T["title"])
st.subheader(T["subtitle"])

# Sidebar - Dropdown menu with instructions
with st.sidebar.expander(T["sidebar_instructions"]):
    st.write(T["instructions"])

st.sidebar.markdown(T["sidebar_powered"], unsafe_allow_html=True)

# Upload PDF file about the topic
uploaded_file = st.file_uploader(T["upload_label"], type="pdf")

if uploaded_file is not None:
    # Extract text from the uploaded PDF file
    pdf_text = extract_text_from_pdf(uploaded_file)

    st.warning(T["welcome"])

    user_input = st.text_input(T["you"], "")

    if st.button(T["ask"]):
        if user_input:
            if openai_api_key:
                response = generate_response(user_input, pdf_text, openai_api_key)
                st.info(f"{T['response_prefix']} {response}")
            else:
                st.error(T["error_api"])
                st.error(T["error_api"])
