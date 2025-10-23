from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

def train_text_classifier(df):
    if len(df) < 10:
        print("Not enough data to train the model")
        return None

    df_model = df.dropna(subset=['description'])
    df_model['label'] = df_model['position'].notnull().astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        df_model['description'],
        df_model['label'],
        test_size=0.2,
        random_state=42
    )

    model = Pipeline([
        ('vect', CountVectorizer(max_features=1000, ngram_range=(1, 2))),
        ('clf', MultinomialNB())
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    print("\nModel report:")
    print(classification_report(y_test, y_pred))
    return model
