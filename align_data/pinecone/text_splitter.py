# dataset/text_splitter.py

from typing import List, Callable, Any
from langchain.text_splitter import TextSplitter
from nltk.tokenize import sent_tokenize


class ParagraphSentenceUnitTextSplitter(TextSplitter):
    """A custom TextSplitter that breaks text by paragraphs, sentences, and then units (chars/words/tokens/etc).
    
    @param min_chunk_size: The minimum number of units in a chunk.
    @param max_chunk_size: The maximum number of units in a chunk.
    @param length_function: A function that returns the length of a string in units.
    @param truncate_function: A function that truncates a string to a given unit length.
    """
    
    DEFAULT_MIN_CHUNK_SIZE = 900
    DEFAULT_MAX_CHUNK_SIZE = 1100
    DEFAULT_TRUNCATE_FUNCTION = lambda string, length, from_end=False: string[-length:] if from_end else string[:length]

    def __init__(
        self, 
        min_chunk_size: int = DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        truncate_function: Callable[[str, int], str] = DEFAULT_TRUNCATE_FUNCTION,
        **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size        

        self._truncate_function = truncate_function

    def split_text(self, text: str) -> List[str]:
        blocks = []
        current_block = ""

        paragraphs = text.split("\n\n")
        for paragraph in paragraphs:
            current_block += "\n\n" + paragraph
            block_length = self._length_function(current_block)

            if block_length > self.max_chunk_size:  # current block is too large, truncate it
                current_block = self._handle_large_paragraph(current_block, blocks, paragraph)
            elif block_length >= self.min_chunk_size:
                blocks.append(current_block)
                current_block = ""
            else:  # current block is too small, continue appending to it
                continue
        
        blocks = self._handle_remaining_text(current_block, blocks)

        return [block.strip() for block in blocks]

    def _handle_large_paragraph(self, current_block, blocks, paragraph):
        # Undo adding the whole paragraph
        current_block = current_block[:-(len(paragraph)+2)]  # +2 accounts for "\n\n"

        sentences = sent_tokenize(paragraph)
        for sentence in sentences:
            current_block += f" {sentence}"
            
            block_length = self._length_function(current_block)
            if block_length < self.min_chunk_size:
                continue
            elif block_length <= self.max_chunk_size:
                blocks.append(current_block)
                current_block = ""
            else:
                current_block = self._truncate_large_block(current_block, blocks, sentence)
        
        return current_block

    def _truncate_large_block(self, current_block, blocks, sentence):
        while self._length_function(current_block) > self.max_chunk_size:
            # Truncate current_block to max size, set remaining sentence as next sentence
            truncated_block = self._truncate_function(current_block, self.max_chunk_size)
            blocks.append(truncated_block)

            remaining_sentence = current_block[len(truncated_block):].lstrip()
            current_block = sentence = remaining_sentence
        
        return current_block

    def _handle_remaining_text(self, current_block, blocks):
        if blocks == []:  # no blocks were added
            return [current_block]
        elif current_block:  # any leftover text
            len_current_block = self._length_function(current_block)
            if len_current_block < self.min_chunk_size:
                # it needs to take the last min_chunk_size-len_current_block units from the previous block
                previous_block = blocks[-1]
                required_units = self.min_chunk_size - len_current_block  # calculate the required units

                part_prev_block = self._truncate_function(previous_block, required_units, from_end=True)  # get the required units from the previous block
                last_block = part_prev_block + current_block

                blocks.append(last_block)
            else:
                blocks.append(current_block)

        return blocks