import streamlit as st
import os
from dotenv import load_dotenv
from huggingface_hub import InferenceClient
from src.api.model_integration import (
    stream_response,
    chat_with_model
)
from src.utils.prompt_templates import (
    get_translation_prompt,
    get_sentiment_analysis_prompt
)

load_dotenv()



def setup_page():
    """
    Sets up the page with custom styles and page configuration.
    """
    st.set_page_config(
        page_title="Advanced Translator",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        :root {
            --llama-color: #4e8cff;
            --llama-color-light: #e6f0ff;
            --llama-color-dark: #1a3a6c;
            --llama-gradient-start: #4e54c8;
            --llama-gradient-end: #8f94fb;
        }
        .stApp {
            margin: auto;
            background-color: var(--background-color);
            color: var(--text-color);
        }
        .logo-container {
            display: flex;
            justify-content: center;
            margin-bottom: 1rem;
        }
        .logo-container img {
            width: 150px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def user_key_handler(user_api_key):
    if user_api_key:
        st.session_state.user_api_key = user_api_key
        client = get_client(st.session_state.user_api_key)
        return client
    else:
        st.error("No key provided. Please paste your API key.")
        
def get_client(user_api_key):
    if user_api_key:
        client = InferenceClient(
            provider="hf-inference",
            api_key=user_api_key,
        )
        if client:
            st.success("Key saved successfully")
            st.session_state.client = client
            return client
        else:
            st.error("Invalid credentials. Please try again.")
            return None
    else:
        st.error("No key provided. Please paste your API key.")
        return None

def key_handler(user_api_key):
    client = get_client(st.session_state.user_api_key)
    st.session_state.client = client
    return client


def main():
    setup_page()

    # Header section with title and subtitle
    st.markdown(
        """
        <div style="text-align: center;">
            <h1 class="header-title">🦙 Translator</h1>
            <p class="header-subtitle">Hugging Face models</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Meta logo
    st.markdown(
        """
        <div class="logo-container">
            <img src="https://www.translatedright.com/wp-content/uploads/2021/10/why-you-shouldnt-use-google-translate-for-business-1-scaled-2560x1280.jpg" alt="Translation Logo">
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Remove the Llama image display

    # Sidebar for settings
    with st.sidebar:
        st.title("🦙 Settings")
        
        user_api_key = st.sidebar.text_area(
            "API Key:",
            height=150,
            width="stretch",
            key="user_api_key"
        )
    
        # returns client object
        key_clicked_client = st.sidebar.button(
            "Save Key",
            on_click=lambda: key_handler(user_api_key),
            key="save_key_btn"
        )
    
        langs = ["eng_Latn", "fra_Latn"]

        source_lang = st.selectbox(
            "From", langs
        )
        st.session_state.source_lang = source_lang

        target_lang = st.selectbox(
            "To", [lang for lang in langs if lang != st.session_state.source_lang]
        )
        st.session_state.target_lang = target_lang

        cultural_context = st.selectbox(
            "Context", ["Formal", "Casual", "Business", "Youth Slang", "Poetic"]
        )
        st.session_state.cultural_context = cultural_context

    # Main container with border
    main_container = st.container(border=True)

    # sidebar results output
    session_results = {"session": None, "results": None, "score": None}

    with main_container:
        st.header("Enter Text for Translation and Analysis")
        text = st.text_area(
            "Text to translate",
            "L'affaire des poisons tourne au cauchemar politique...",
            height=200,
        )
        st.caption(f"Character count: {len(text)}")
        st.session_state.messages = text

        if st.button("Translate and Analyze", type="primary", key="translate_and_analyze_btn"):
            if text and st.session_state.client:
                st.session_state.translate_and_analyze_btn = True
                # Tabs for different analysis types
                tab1, tab2 = st.tabs(
                    [
                        "Translation",
                        "Sentiment Analysis"
                    ]
                )
               
                # Tab 1: Translation
                with tab1:
                    st.subheader("Result")
                    translation_container = st.empty()
                    translation_prompt = get_translation_prompt(st.session_state.messages)
                    st.session_state.translation_prompt = translation_prompt
                    #print("translation_prompt: ", translation_prompt)
                    
                    try:
                        translation = stream_response(
                            translation_prompt,
                            translation_container,
                            1
                        )      
                                     

                    except Exception as e:
                        print("ret_trans_except: ", e)
                
                #Tab 2: Sentiment Analysis
                with tab2:
                    st.subheader("Result")
                    sentiment_container = st.empty()
                    sentiment_prompt = get_sentiment_analysis_prompt(st.session_state.messages)
                    st.session_state.sentiment_prompt = sentiment_prompt
                    #print("sentiment_prompt: ", sentiment_prompt)
                    
                    try:
                        sentiment = stream_response(
                            sentiment_prompt,
                            sentiment_container,
                            2
                        )         
                                
                    except Exception as e:
                        print("ret_sentiment_except: ", e)
            else:
                st.error("Please enter key!")
                
    # Sidebar for additional information and feedback
    with st.sidebar:
        st.subheader("About")
        st.info("Demo app - Hugging Face models")


def init_state():
    defaults = {
        "key_clicked": False,
        "user_api_key": None,
        "model_id": None,
        "model_id_en_fr": "Helsinki-NLP/opus-mt-tc-big-en-fr",
        "model_id_fr_en": "Helsinki-NLP/opus-mt-fr-en",
        "model_id_sentiment_analysis": "tabularisai/multilingual-sentiment-analysis",
        "client": None,
        "result": None,
        "params": {
            "src_lang": "en",
            "tgt_lang": "fr",
            "clean_up_tokenization_spaces": True,
            "truncation": "do_not_truncate",
            "generate_parameters": {
                "temperature": 0.5,
            }
        },
        "messages": None,
        "translation_prompt": None,
        "source_lang": "en",
        "target_lang": "fr",
        "cultural_context": "formal",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

if __name__ == "__main__":
    init_state()
    main()
    