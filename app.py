import streamlit as st

# Importa los dashboards de cada módulo
from modules.pdf_viewer import pdf_viewer_dashboard
from modules.crm import crm_dashboard
from modules.content_manager import content_manager_dashboard

st.set_page_config(page_title="CRM-GPT", page_icon="🤖", layout="wide")

def main():
    st.title("CRM-GPT: Navegación")

    tab = st.sidebar.radio(
        "Selecciona un módulo",
        ("PDF Viewer", "CRM", "Content Manager"),
        key="main_nav"
    )

    if tab == "PDF Viewer":
        pdf_viewer_dashboard()
    elif tab == "CRM":
        crm_dashboard()
    elif tab == "Content Manager":
        content_manager_dashboard()

if __name__ == "__main__":
    main()
