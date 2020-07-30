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
exp.parameter('clf', LogisticRegression()) \
        .addSearch([SVC()])

exp.runSearch()
```
"""
import collections


ParamSearch = collections.namedtuple('ParamSearch', ('name', 'alternatives', 'include_default'))


class DescribedValue:
    def __init__(self, value):
        if isinstance(value, tuple):
            self._desc, self.value = value
        else:
            self.value = value
            self._desc = None

    def __repr__(self):
        return self._desc or str(self.value)


class Parameter:
    def __init__(self, name, default_value):
        self.name = name
        self.default = DescribedValue(default_value)
        self.searches = []

    def setDefault(self, value):
        self.default = DescribedValue(value)
        return self

    def addSearch(self, alternatives, search_name=None, include_default=True):
        name = search_name or self.name
        if name in [search.name for search in self.searches]:
            raise ValueError(f'duplicate name: {name}')

        alternatives = [DescribedValue(alt) for alt in alternatives]
        self.searches.append(ParamSearch(name, alternatives, include_default))
        return self

    def alternatives(self, search):
        alt_list = search.alternatives
        if search.include_default:
            alt_list = [self.default] + alt_list

        return alt_list


class Evaluation:
    def __init__(self, run_experiment, aggregate_results=None):
        self._run_experiment = run_experiment
        self._aggregate_results = aggregate_results
        self._params = {}

    def parameter(self, name, default_value=None):
        if name not in self._params:
            self._params[name] = Parameter(name, default_value)

        return self._params[name]

    def defaultValues(self):
        return dict((param.name, param.default.value) for param in self._params.values())

    def findName(self, name):
        for param in self._params.values():
            for search in param.searches:
                if search.name == name:
                    return param, search

        raise ValueError(f'name not found: {name}')

    def getFullGrid(self, names, default_values=None):
        param_values = default_values or []

        if not names:
            yield param_values

        for i in range(len(names)):
            param, search = self.findName(names[i])
            alternatives = param.alternatives(search)

            for alt in alternatives:
                alt_params = list(param_values)
                alt_params.append((names[i], param.name, alt))

                yield from self.getFullGrid(names[i+1:], alt_params)

    def runFullGrid(self, names):
        self.runExperiments(self.getFullGrid(names))

    def getSearch(self, names=None):
        for param in self._params.values():
            for search in param.searches:
                if names is None or search.name in names:
                    alternatives = param.alternatives(search)

                    for alt in alternatives:
                        yield [(search.name, param.name, alt)]

    def runSearch(self, name=None):
        self.runExperiments(self.getSearch(name))

    def runExperiments(self, experiments):
        results = []
        default_values = self.defaultValues()

        for param_set in experiments:
            param_values = default_values.copy()
            param_values.update(dict((param, value.value) for name, param, value in param_set))

            selected_params = [(search_name, value) for search_name, param, value in param_set]
            desc = ', '.join([f'{name}={value}' for name, param, value in param_set])

            retval = self._run_experiment(**param_values, desc=desc)
            results.append((selected_params, retval))

        if self._aggregate_results and results:
            self._aggregate_results(results)
