# Multi Procces normalizer for Windows 

import pandas as pd
from pandarallel import pandarallel

df_raw_text = pd.read_csv("dataset.csv")

df_normalized_text = pd.DataFrame()

pandarallel.initialize(progress_bar=True)

# Procces and apply all text
def preprocess_text(text):
    import re
    from zemberek import (
    TurkishSentenceNormalizer,
    TurkishMorphology,
    )
    
    # Zemberek Normalizer
    morphology = TurkishMorphology.create_with_defaults()
    normalizer = TurkishSentenceNormalizer(morphology)
    
    # Apply Zemberek Normalizer
    normalized_text = normalizer.normalize(text)

    ## Clean all Links
    normalized_text = re.sub(r'[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)', '', normalized_text)
   
    ## Clean unnecessary spaces
    normalized_text = re.sub(r'\s+', ' ', normalized_text).strip()
   
    ## Clean mentions ve hashtags
    normalized_text = re.sub(r'@\w+|#\w+', '', normalized_text)

    ## Clean punct.
    # normalized_text = re.sub(r'[^\w\s]', '', normalized_text)
    
    return normalized_text

print(f"Size of raw text : {df_raw_text.size}")

df_normalized_text['text'] = df_raw_text['text'].parallel_apply(preprocess_text)

# See some result :D
print(df_normalized_text.head(20))

df_normalized_text.to_csv("normalized_dataset.csv", encoding='utf-8')
