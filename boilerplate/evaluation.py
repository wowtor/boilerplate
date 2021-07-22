"""
Example of use:

```python
def run_experiment(desc, data, clf):
    clf.fit(data.X, data.y)
    ...
    return results

def aggregate_results(results):
    ...

exp = Evaluation(run_experiment, aggregate_results)
exp.parameter('data', my_data)
exp.parameter('clf', LogisticRegression())

exp.runParameterSearch("clf", [LogisticRegression(), SVC()])
```
"""
class DescribedValue:
    def __init__(self, value):
        if isinstance(value, tuple):
            self._desc, self.value = value
        else:
            self.value = value
            self._desc = None

    def __repr__(self):
        return self._desc or str(self.value)


class DescribedDict(dict):
    def __init__(self, desc, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.desc = desc

    def __repr__(self):
        return self.desc


class Setup:
    def __init__(self, evaluate, aggregate_results=None):
        self._evaluate = evaluate
        self._aggregate_results = aggregate_results
        self._params = {}

    def parameter(self, name, default_value=None):
        """
        Defines a parameter with name `name` and optionally a default value.
        """
        self._params[name] = DescribedValue(default_value)
        return self

    def defaultValues(self):
        """
        Returns a dictionary with default parameter names and values as key/value.
        """
        return dict((name, described_value.value) for name, described_value in self._params.items() if described_value.value is not None)

    def getFullGrid(self, dimensions, default_values=None):
        raise ValueError("not implemented")

    def runFullGrid(self, dimensions, default_values=None):
        """
        Runs a full grid of experiments along the dimensions in `names`.
        """
        self.runExperiments(self.getFullGrid(dimensions), default_values)

    def runParameterSearch(self, name, values, default_values=None, aggregate_kw={}):
        """
        Runs a series of experiments, varying a single parameter value.
        """
        experiments = [{name: DescribedValue(value)} for value in values]
        self.runExperiments(experiments, default_values=default_values, aggregate_kw=aggregate_kw)

    def runDefaults(self):
        return self.runExperiment({})[1]

    def runExperiment(self, param_set, default_values=None):
        """
        Runs a single experiment.
        
        Parameters:
        -----------
        param_set: a dictionary, with parameter names as keys, and `DescribedValue`s as values
        default_values: a dictionary, with parameter names as keys, and their values as values
        """
        param_values = default_values.copy() if default_values is not None else self.defaultValues()
        for name, value in param_set.items():
            param_values[name] = value.value

        result = self._evaluate(**param_values, selected_params=param_set)
        return param_set.items(), result

    def runExperiments(self, experiments, default_values=None, aggregate_kw={}):
        """
        Carry out a range of experiments, for example varying one parameter.
        """
        results = []

        for param_set in experiments:
            results.append(self.runExperiment(param_set, default_values))

        if self._aggregate_results and results:
            self._aggregate_results(results, **aggregate_kw)
