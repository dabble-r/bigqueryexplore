from logging import PlaceHolder
import streamlit as st
import time
from datetime import datetime


def get_state():
    return st.session_state

def chat_with_model(prompt, container, tab):
    """
    Send a chat request to Watson X and display the response.
    
    Args:
        messages: List of message dicts with 'role' and 'content'
        model_id: The model identifier to use
        container: Streamlit container for displaying response
        
    Returns:
        str: The response text, or None if error
    """
    state = get_state()
    model_id = state.model_id
    client = state.client
    params = state.params
    source_lang = state.source_lang
    target_lang = state.target_lang
    user_api_key = state.user_api_key
    #print("prompt: ", prompt)
    print("source_lang: ", source_lang)
    #print("target_lang: ", target_lang)
    print("model_id: ", model_id)
    
    try:
        response_placeholder = container.empty()
        response_placeholder.info("🔄 Processing request...")

        # Hugging Face T5 call
        current_datetime = datetime.now().time().strftime("%H:%M:%S")

        ret_messages = {"task": {"translation": None, "sentiment": None}}
        
        if tab == 1: # Translation
            # Translation model selection
            if source_lang == "eng_Latn":
                st.session_state.model_id = st.session_state.model_id_en_fr
                result = client.translation(
                        prompt,
                        model=st.session_state.model_id_en_fr,
                        src_lang=source_lang,
                        tgt_lang=target_lang
                    )
                translation_result = result.translation_text
                split_trans = translation_result.split()[2:]
                joined_trans = " ".join(split_trans)
                # response_placeholder.markdown(" ".join(split_trans))
                # print(
                #   "result:", 
                #   {"translation": translation,
                #    "split_trans" translation.split(),
                #   .join(split_trans), 
                #    "time": current_datetime}
                # )
                ret_messages['task']['translation'] = joined_trans

            elif source_lang == "fra_Latn":
                st.session_state.model_id = st.session_state.model_id_fr_en
                result = client.translation(
                        prompt,
                        model=st.session_state.model_id_fr_en,
                        src_lang=source_lang,
                        tgt_lang=target_lang
                    )
                translation_result = result.translation_text
                split_trans = translation_result.split()[1:]
                joined_trans = " ".join(split_trans)
                #response_placeholder.markdown(" ".join(split_trans))
                # print(
                #   "result:", 
                #   {"translation": translation,
                #    "split_trans" translation.split(),
                #   .join(split_trans), 
                #    "time": current_datetime}
                # )
                ret_messages['task']['translation'] = joined_trans
                response_format = f"""
                                Translation: {ret_messages['task']['translation']}
                               """
                response_placeholder.markdown(response_format)

        elif tab == 2: # Sentiment Analysis
            # Sentiment analysis model selection
            sentiment = client.text_classification(
                prompt,
                model=st.session_state.model_id_sentiment_analysis,
            )
            # print("sentiment: ", sentiment[0])
            
            sentiment_by_score = (sentiment[0]['label'], sentiment[0]['score'])
            # print("sentiment: ", sentiment_by_score)

            ret_messages['task']["sentiment"] = sentiment_by_score

            response_format = f"""
                                Sentiment: {ret_messages['task']['sentiment'][0]}\n\n
                                Score: {ret_messages['task']['sentiment'][1]}
                               """
            response_placeholder.markdown(response_format)

        return ret_messages

    except Exception as e:
        container.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def stream_response(prompt, container, tab):
    """
    Main entry point for streaming responses.
    Routes to Watson X chat.
    
    Args:
        messages: List of message dicts with 'role' and 'content'
        container: Streamlit container for displaying response
        model_name: Model identifier from dropdown
        
    Returns:
        str: Response text or None
    """
    
    return chat_with_model(prompt, container, tab)
