import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error, r2_score


warnings.filterwarnings("ignore")

# File paths
data_dict_path = "/kaggle/input/datasets/nudratabbas/software-developer-salary-prediction-dataset/data_dictionary.csv"
train_path = "/kaggle/input/datasets/nudratabbas/software-developer-salary-prediction-dataset/train.csv"
test_path = "/kaggle/input/datasets/nudratabbas/software-developer-salary-prediction-dataset/test.csv"

# Load Data
data_dict = pd.read_csv(data_dict_path)
train_df = pd.read_csv(train_path)
test_df = pd.read_csv(test_path)

# Shape
print("Train Shape:", train_df.shape)
print("Test Shape:", test_df.shape)

train_df.head()

train_df.isnull().sum()
train_df.columns

for col in train_df.select_dtypes(include="object").columns:
    print(f"\ncol: {col}")
    print(train_df[col].value_counts())
    print("\n")

data_dict.head()
train_df.fillna(method="ffill", inplace=True)
train_df.drop_duplicates(inplace=True)

train_df.info()
print("\nMissing Values Per Column\n:", train_df.isnull().sum())
print("\n\n")

sns.set_style("whitegrid")

plt.rcParams["figure.figsize"] = (10, 6)
sns.histplot(train_df["salary_usd"], kde=True, color="Purple")
plt.title("Salary Distribution")
plt.xlabel("Salary (USD)")
plt.ylabel("Frequency")
plt.show()


sns.scatterplot(x="experience_level", y="salary_usd", data=train_df, color="Blue")
plt.title("Experience vs Salary")
plt.show()

country_salary = (
    train_df.groupby("company_location")["salary_usd"]
    .mean()
    .sort_values(ascending=False)
    .head(10)
)
print(country_salary)

sns.barplot(x=country_salary.values, y=country_salary.index, palette="Viridis")
plt.title("Top 10 Countries by Average Salary")
plt.xlabel("Average Salary (USD)")
plt.ylabel("Country")
plt.show()

sns.boxplot(x="education", y="salary_usd", data=train_df, palette="coolwarm")
plt.title("Education Level vs Salary")
plt.xticks(rotation=45)
plt.show()

sns.boxplot(x="company_size", y="salary_usd", data=train_df, palette="Set2")
plt.title("Company Size vs Salary")
plt.show()

lang_series = train_df["languages"].dropna().str.split(",").explode()
top_langs = lang_series.value_counts().head(10)
print(top_langs)

sns.barplot(x=top_langs.values, y=top_langs.index, palette="magma")
plt.title("Top 10 Languages")
plt.show()

framework_series = train_df["frameworks"].dropna().str.split(",").explode()
top_frameworks = framework_series.value_counts().head(10)
sns.barplot(x=top_frameworks.values, y=top_frameworks.index, palette="cubehelix")
plt.title("Top 10 Frameworks")
plt.show()

numeric_df = train_df.select_dtypes(include=["float64", "int64"])
sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.show()

sns.boxplot(x="experience", y="salary_usd", data=train_df, palette="rainbow")
plt.title("Salary Distribution by Experience")
plt.xticks(rotation=45)
plt.show()

sns.countplot(x="education", data=train_df, palette="Set3")
plt.title("Education Distribution")
plt.xticks(rotation=45)
plt.show()

sns.pairplot(train_df, diag_kind="kde")
plt.show()

train = train_df.copy()
cat_cols = ["country", "education", "languages", "frameworks", "company_size"]

for col in cat_cols:
    le = LabelEncoder()
    train[col] = le.fit_transform(train[col].astype(str))

print("Encoding Done")

X = train.drop("salary_usd", axis=1)
y = train["salary_usd"]

X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
print("Splitting Done")

models = {
    "Linear Regression": LinearRegression(),
    "Decision Tree": DecisionTreeRegressor(random_state=42),
    "Random Forest": RandomForestRegressor(n_estimators=200, random_state=42),
}

results = []

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)

    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    r2 = r2_score(y_val, y_pred)

    results.append(
        {
            "Model": name,
            "RMSE": rmse,
            "R2 Score": r2,
        }
    )

    print(f"{name}:")
    print(f"  RMSE: {rmse:.2f}")
    print(f"  R2 Score: {r2:.2f}")
    print("\n")

results_df = pd.DataFrame(results, columns=["Model", "RMSE", "R2 Score"])
results_df

sns.barplot(x="Model", y="R2 Score", data=results_df, palette="viridis")
plt.title("Model Comparison(R2 Score)")
plt.xticks(rotation=30)
plt.show()
