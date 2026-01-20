
def get_translation_prompt(text, source_lang, target_lang):
    return f"translate {source_lang} to {target_lang}:\n\n{text}"

def get_sentiment_analysis_prompt(text, source_lang):
    """
    Returns a prompt for conducting sentiment analysis on a given text.
    """
    return f"""
    Conduct a comprehensive sentiment analysis of the following {source_lang} text:

    "{text}"

    Provide your analysis in markdown format as follows:

    ## :blue[Overall Sentiment]
    [Positive/Negative/Neutral/Mixed]

    ## :green[Sentiment Breakdown]
    - **Positivity**: :smile: [Score from 0 to 1]
    - **Negativity**: :frowning: [Score from 0 to 1]
    - **Neutrality**: :neutral_face: [Score from 0 to 1]

    ## :orange[Key Emotional Indicators]
    1. **:heart: [Emotion 1]**: 
      - _Evidence_: ":violet[Relevant quote from text]"
      - _Explanation_: [Brief analysis]

    ## :earth_americas: Cultural Context
    [Explain how the sentiment might be perceived in the {source_lang}-speaking culture, considering any cultural-specific expressions or connotations]
    """

def get_cultural_reference_explanation_prompt(text, source_lang, target_lang):
    """
    Returns a prompt to explain cultural references in a source language for a target language audience.
    """
    return f"""
    As a cross-cultural communication expert, explain the cultural references in this {source_lang} text for someone from a {target_lang} background:

    "{text}"

    ## :earth_americas: Cultural References

    1. **:star: [Reference 1]**
       - _Meaning_: :blue[Explanation]
       - _Cultural Significance_: :green[Brief description]
       - _{target_lang} Equivalent_: :orange[Equivalent or similar concept, if applicable]
       - _Usage Example_: ":violet[Show how it's used in a sentence]"

    2. **:star: [Reference 2]**
       - _Meaning_: :blue[Explanation]
       - _Cultural Significance_: :green[Brief description]
       - _{target_lang} Equivalent_: :orange[Equivalent or similar concept, if applicable]
       - _Usage Example_: ":violet[Show how it's used in a sentence]"

    ## :globe_with_meridians: Overall Cultural Context
    [Summarize the cultural differences relevant to this text.]
    """

def get_interactive_translation_prompt(text, source_lang, target_lang):
    """
    Returns a prompt for providing an interactive, detailed translation with context.
    """
    return f"""
    Translate the following text from {source_lang} to {target_lang} and provide an overall analysis of its meaning, usage, and cultural relevance:

    "{text}"

    ## :books: General Translation
    **Text** → ":blue[Overall translation]"

    ## :arrows_counterclockwise: Contextual Usage and Adaptation
    1. ":green[Context 1]" - _Explanation_: [How the translation adapts to cultural context]
    2. ":orange[Context 2]" - _Explanation_: [Alternative contextual usage]

    ## :dna: Etymology and Origin
    - **Origin**: :violet[Brief description of word origins or key concepts]
    - **Related concepts**: :rainbow[If applicable, related words or phrases]

    ## :memo: Usage Notes
    - **Register**: :blue[Formal/Informal/etc.]
    - **Connotations**: :green[Positive/Negative connotations of the translation]
    - **Cultural Significance**: :orange[Explain the cultural impact or relevance of the translation]
    """

## education oriented prompt templates
def get_language_standards(text, source_lang, target_lang):
    pass 

def get_grammar_focus(text, source_lang, target_lang):
    """
    Builds a structured prompt for translation, verb/tense comparison,
    and cross-linguistic grammar analysis.
    """
    return f"""
        You are a multilingual translation and grammar expert.

        Your task is to:
        1. Translate the text from **{source_lang}** to **{target_lang}**.
        2. Identify and explain the key **verbs**, **tenses**, and **grammatical structures** in the source text.
        3. Compare how those same verbs/tenses/structures are expressed in the target language.
        4. Highlight any cultural or contextual nuances that influence the translation.

        Text to analyze:
        "{text}"

        Respond in the following markdown structure (optimized for Streamlit):

        ## :blue[Translation]
        > Provide the full translation here.

        ## :orange[Verb & Tense Comparison]
        List each important verb or grammatical structure from the source text and compare it to the target language.

        Format each item like this:
        - **Source**: ":violet[original phrase]"
        - **Target**: ":rainbow[translated verb/structure]"
        - **Tense/Aspect**: Describe the tense/aspect in both languages.
        - **Explanation**: Briefly explain why this tense/structure is used in each language and how they differ.

        ## :red[Linguistic Analysis]
        - **Register**: Formal, informal, neutral, etc.
        - **Tone**: Describe the tone in both languages.
        - **Structural Differences**: Key grammar differences between {source_lang} and {target_lang}.
        - **Cultural Considerations**: Any cultural or contextual elements that shaped the translation.
        - **Challenges**: Note any tricky grammar, idioms, or tense mismatches.
        """

# testing new prompt (ed streamlit project)
def get_comms_focus(text, source_lang, target_lang):
    """
    Builds a structured prompt for translation, verb/tense comparison,
    and cross-linguistic grammar analysis.
    """
    return f"""
        You are a multilingual translation assistant and grammar expert.

        Your task is to:
        1. Translate the text from **{source_lang}** to **{target_lang}**.
        2. Identify and explain the key **verbs**, **tenses**, and **grammatical structures** in the source text.
        3. Compare how those same verbs/tenses/structures are expressed in the target language (if applicable).
        4. Highlight any cultural or contextual nuances that influence the translation.

        Text to analyze:
        "{text}"

        Respond in the following markdown structure (optimized for Streamlit):

        ## :blue[Translation]
        > Provide the full translation here.

        ## :orange[Verb & Tense Comparison]
        List each important verb or grammatical structure from the source text and compare it to the target language.

        Format each item like this:
        | **Source**      | **Target**      | **Tense/Aspect** | **Explanation** |
        |    :----:       |    :----:       |     :----:       |     :----:      |
        | -----------     | -----------     |  -----------     |    -----------  |

        - **Source**: ":violet[original phrase]"
        - **Target**: ":rainbow[translated verb/structure]"
        - **Tense/Aspect**: Describe the tense/aspect in both languages.
        - **Explanation**: Briefly explain why this tense/structure is used in each language and how they differ.

        ## :red[Linguistic Analysis]
        - **Register**: Formal, informal, neutral, etc.
        - **Tone**: Describe the tone in both languages.
        - **Structural Differences**: Key grammar differences between {source_lang} and {target_lang}.
        - **Cultural Considerations**: Any cultural or contextual elements that shaped the translation.
        - **Challenges**: Note any tricky grammar, idioms, or tense mismatches.
        """