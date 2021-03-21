from sklearn import ensemble, gaussian_process, svm, linear_model, naive_bayes, discriminant_analysis
from sklearn.model_selection import StratifiedKFold, cross_val_score, GridSearchCV
import pandas as pd

# classifiers
classifiers = [
    # Ensemble Methods
    ensemble.AdaBoostClassifier(),
    ensemble.BaggingClassifier(),
    ensemble.ExtraTreesClassifier(),
    ensemble.GradientBoostingClassifier(),
    ensemble.RandomForestClassifier(),

    # Gaussian Processes
    gaussian_process.GaussianProcessClassifier(),

    # GLM
    linear_model.LogisticRegressionCV(),

    # Navies Bayes
    naive_bayes.BernoulliNB(),
    naive_bayes.GaussianNB(),

    # SVM
    svm.SVC(probability=True),
    svm.NuSVC(probability=True),

    # Discriminant Analysis
    discriminant_analysis.LinearDiscriminantAnalysis(),
    discriminant_analysis.QuadraticDiscriminantAnalysis(),

    # xgboost: http://xgboost.readthedocs.io/en/latest/model.html
]


def find_best_algorithms(dx, dy, classifier_list=classifiers):
    print('Finding Best algo')
    # This function is adapted from https://www.kaggle.com/yassineghouzam/titanic-top-4-with-ensemble-modeling
    # Cross validate model with Kfold stratified cross validation
    kfold = StratifiedKFold(n_splits=5)

    # Grab the cross validation scores for each algorithm

    print('\t Calc Result')
    cv_results = [cross_val_score(classifier, dx, dy, scoring="neg_log_loss", cv=kfold) for classifier in classifier_list]

    print('\t Calc Mean')
    cv_means = [cv_result.mean() * -1 for cv_result in cv_results]

    print('\t Calc Std deviation')
    cv_std = [cv_result.std() for cv_result in cv_results]
    algorithm_names = [alg.__class__.__name__ for alg in classifiers]

    # Create a DataFrame of all the CV results
    cv_results = pd.DataFrame({
        "Mean Log Loss": cv_means,
        "Log Loss Std": cv_std,
        "Algorithm": algorithm_names
    })

    return cv_results.sort_values(by='Mean Log Loss').reset_index(drop=True)


def optimise_hyperparameters(train_x, train_y, algorithms, parameters):
    kfold = StratifiedKFold(n_splits=5)
    best_estimators = []

    for alg, params in zip(algorithms, parameters):
        gs = GridSearchCV(alg, param_grid=params, cv=kfold, scoring='neg_log_loss', verbose=1)
        gs.fit(train_x, train_y)
        best_estimators.append(gs.best_estimator_)
    return best_estimators


__known_best_params__ = {
    'base_estimator': None,
    'bootstrap': True,
    'bootstrap_features': False,
    'max_features': 1.0,
    'max_samples': 0.2,
    'n_estimators': 10,
    'n_jobs': None,
    'oob_score': False,
    'random_state': None,
    'verbose': 0,
    'warm_start': False
}