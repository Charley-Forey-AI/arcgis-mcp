from pydantic import BaseModel
from typing import List, Tuple, Optional, Dict


class textclassifierprompt(BaseModel):
    examples: Optional[Dict[str, List]]
    prompt: Optional[str]
    labels: Optional[List[str]]

    def _valid(self):
        # check if label and example both are missing
        if self.examples is None and self.labels is None:
            raise ValueError("The value of both labels and examples cannot be None.")

    def _format_example(self):
        if self.examples is not None:
            temp = []
            for k, v in self.examples.items():
                for entry in v:
                    temp.append(f"{entry}\n\n {k}")
            self.examples = temp
        return self.examples


class nerprompt(BaseModel):
    examples: Optional[List[Tuple[str, Dict[str, List]]]]
    prompt: Optional[str]
    labels: Optional[List[str]]

    def _valid(self):
        # check if label and example both are missing
        if self.examples is None and self.labels is None:
            raise ValueError("The value of both labels and examples cannot be None.")

    def _format_example(self):
        if self.examples is not None:
            temp = []
            for i in self.examples:
                tstrin = str(i[1])
                temp.append(f"{i[0]}\n\n {tstrin}")
            self.examples = temp
        return self.examples


class seqtoseqprompt(BaseModel):
    examples: Optional[List[Tuple[str, str]]]  # Pydantic evaluates a Tuple as a List.
    # This type of schema supports restricting the number of elements.
    prompt: Optional[str]

    def _valid(self):
        # check if label and example both are missing
        if self.examples is None:
            raise ValueError("The value of examples cannot be None.")

    def _format_example(self):
        temp = []
        for i in self.examples:
            temp.append(f"{i[0]} \n\n {i[1]}")
        self.examples = temp
        return self.examples
