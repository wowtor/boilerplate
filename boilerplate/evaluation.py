"""
Example of use:

```python
def run_experiment(selected_params, data, clf):
    clf.fit(data.X, data.y)
    ...
    return results


# Parameter search
exp = Setup(run_experiment)
exp.parameter('data', my_data)
exp.parameter('clf', LogisticRegression())
for selected_params, param_values, results in exp.runParameterSearch("clf", [LogisticRegression(), SVC()]):
    # do something sensible with the results
    ...


# Grid search
exp = Setup(run_experiment)
exp.parameter('data', my_data)  # Default value for parameter
exp.parameter('clf', LogisticRegression())  # Default value for parameter
for selected_params, param_values, results in exp.runFullGrid({'n_most_common_words': [5, 10, 20, 30],
                'binning_strategy', [regular_binning_strategy, mismatch_binning_strategy]}):
    # do something sensible with the results
    ...
"""
import collections
import itertools
import logging
from collections import Callable
from typing import Optional, List, Any, Dict, Union

LOG = logging.getLogger(__name__)


class DescribedValue:
    def __init__(self, value: Any, desc: Optional[str] = None):
        if isinstance(value, DescribedValue):
            self.value = value.value
            self._desc = desc or value._desc
        else:
            self.value = value
            self._desc = desc

    def __repr__(self):
        return self._desc or str(self.value)


class Setup:
    def __init__(self, evaluate: Callable):
        self._evaluate = evaluate
        self._default_values = {}

    def parameter(self, name: str, default_value=None):
        """
        Defines a parameter with name `name` and optionally a default value.
        """
        self._default_values[name] = DescribedValue(default_value)
        return self

    def defaultValues(self):
        """
        Returns a dictionary with default parameter names and values as key/value.
        """
        return {name: described_value.value for name, described_value in self._default_values.items()}

    def runFullGrid(self, parameter_ranges: Dict[str, List[Any]], default_values: Optional[Dict[str, Any]] = None):
        """
        Runs a full grid of experiments along the dimensions in `names`.

        Parameters:
            - parameter_ranges: `dict` of parameters included in gridsearch; keys are parameter names, values are lists
              of parameter values
            - default_values: any predefined parameter values
        """
        parameter_ranges = collections.OrderedDict(parameter_ranges.items())
        combinations = itertools.product(*parameter_ranges.values())

        experiments = [
            collections.OrderedDict([(dim, DescribedValue(combi[i])) for i, dim in enumerate(parameter_ranges.keys())])
            for combi in combinations]
        yield from self.runExperiments(experiments, default_values=default_values)

    def runParameterSearch(self, name: str, values: List[Any], default_values: Optional[Dict[str, Any]] = None):
        """
        Runs a series of experiments, varying a single parameter value.

        Parameters:
            - name: name of parameters for the search
            - values: the values to try out in this search
            - default_values: any predefined parameter values
        """
        experiments = [{name: DescribedValue(value)} for value in values]
        yield from self.runExperiments(experiments, default_values=default_values)

    def runDefaults(self):
        """
        Runs an experiment with the default values and returns the result
        """
        return self.runExperiment({})[2]

    def runExperiment(self, param_set: Dict[str, Any], default_values: Optional[Dict[str, Any]] = None):
        """
        Runs a single experiment.
        
        Parameters:
        -----------
            - param_set: a dictionary, with parameter names as keys
            - default_values: any predefined parameter values
        """
        defaults = self.defaultValues()
        if default_values is not None:
            defaults.update(default_values)

        param_values = defaults.copy()
        for name, value in param_set.items():
            param_values[name] = value.value

        result = self._evaluate(**param_values, selected_params=param_set)
        return param_set, param_values, result

    def runExperiments(self, experiments: Union[List[Dict[str, Any]], Dict[str, Any]], default_values=None):
        """
        Carry out a range of experiments, for example varying one parameter or doing a gridsearch.

        Parameters:
        -----------
            - experiments: a list of experiments to run
            - default_values: any predefined parameter values
        """
        # Run the experiments
        for param_set in experiments:
            try:
                yield self.runExperiment(param_set, default_values)
            except Exception as e:
                LOG.warning(f"experiment aborted: {e}; params: {param_set}")
