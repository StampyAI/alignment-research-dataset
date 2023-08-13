# dataset/text_splitter.py

from typing import List, Callable, Any
from langchain.text_splitter import TextSplitter
from nltk.tokenize import sent_tokenize

# TODO: Fix this.
# sent_tokenize has strange behavior sometimes: 'The units could be anything (characters, words, sentences, etc.), depending on how you want to chunk your text.'
# splits into ['The units could be anything (characters, words, sentences, etc.', '), depending on how you want to chunk your text.']

StrToIntFunction = Callable[[str], int]
StrIntBoolToStrFunction = Callable[[str, int, bool], str]

def default_truncate_function(string: str, length: int, from_end: bool = False) -> str:
    return string[-length:] if from_end else string[:length]

class ParagraphSentenceUnitTextSplitter(TextSplitter):
    """A custom TextSplitter that breaks text by paragraphs, sentences, and then units (chars/words/tokens/etc).
    
    @param min_chunk_size: The minimum number of units in a chunk.
    @param max_chunk_size: The maximum number of units in a chunk.
    @param length_function: A function that returns the length of a string in units. Defaults to len().
    @param truncate_function: A function that truncates a string to a given unit length.
    """
    
    DEFAULT_MIN_CHUNK_SIZE: int = 900
    DEFAULT_MAX_CHUNK_SIZE: int = 1100
    DEFAULT_LENGTH_FUNCTION: StrToIntFunction = len
    DEFAULT_TRUNCATE_FUNCTION: StrIntBoolToStrFunction = default_truncate_function

    def __init__(
        self, 
        min_chunk_size: int = DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        length_function: StrToIntFunction = DEFAULT_LENGTH_FUNCTION,
        truncate_function: StrIntBoolToStrFunction = DEFAULT_TRUNCATE_FUNCTION,
        **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        assert self.min_chunk_size <= self.max_chunk_size, "min_chunk_size must be less than or equal to max_chunk_size"

        self._length_function = length_function
        self._truncate_function = truncate_function

    def split_text(self, text: str) -> List[str]:
        """Split text into chunks of length between min_chunk_size and max_chunk_size."""
        blocks: List[str] = []
        current_block: str = ""

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

    def _handle_large_paragraph(self, current_block: str, blocks: List[str], paragraph: str) -> str:
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
                current_block = self._truncate_large_block(current_block, blocks)
        return current_block

    def _truncate_large_block(self, current_block: str, blocks: List[str]) -> str:
        while self._length_function(current_block) > self.max_chunk_size:
            # Truncate current_block to max size, set remaining text as current_block
            truncated_block = self._truncate_function(current_block, self.max_chunk_size)
            blocks.append(truncated_block)

            current_block = current_block[len(truncated_block):].lstrip()
        
        return current_block

    def _handle_remaining_text(self, last_block: str, blocks: List[str]) -> List[str]:
        if blocks == []:  # no blocks were added
            return [last_block]
        elif last_block:  # any leftover text
            len_last_block = self._length_function(last_block)
            len_to_add_to_last_block_from_prev_block = self.min_chunk_size - len_last_block
            if len_to_add_to_last_block_from_prev_block > 0:
                # Add text from previous block to last block if the last_block is too short
                part_prev_block = self._truncate_function(
                    string=blocks[-1], 
                    length=len_to_add_to_last_block_from_prev_block, 
                    from_end=True
                )
                last_block = part_prev_block + last_block

            blocks.append(last_block)

        return blocks
    

if __name__ == '__main__':
    #Test
    splitter = ParagraphSentenceUnitTextSplitter()
    text = """This is a TextSplitter class implementation which is used for dividing a piece of text into chunks based on certain criteria. It inherits from another TextSplitter class and overrides some of its methods to provide custom text splitting functionality. Here's a high-level overview: The TextSplitter receives a string of text and splits it into blocks, with each block being a sequence of paragraphs, sentences, and then units (chars/words/tokens/etc). The split_text method is the main method where the splitting occurs. It starts by splitting the text into paragraphs and then iteratively goes through each paragraph, checking if the length of the current block of text is larger than max_chunk_size or smaller than min_chunk_size, and acting accordingly. If the current block becomes too large, the _handle_large_paragraph method is called, which reverts the addition of the last paragraph and splits it into sentences instead, adding them one by one to the current block. If adding a sentence makes the block too large, the _truncate_large_block method is called. It repeatedly truncates the block to the max_chunk_size and moves the remaining text to the next block until the block is small enough. After all paragraphs have been processed, the _handle_remaining_text method is called to handle any text that didn't make it into a block. The a = b = c pattern is used in Python to assign the same value to multiple variables at once. In this case, current_block = sentence = remaining_sentence is setting both current_block and sentence to the value of remaining_sentence. This means that in the next iteration, both current_block and sentence will start as the remaining part of the sentence that didn't fit into the previous block. The _truncate_function is used to truncate a string to a certain length. By default, it either takes the first or last length characters from the string, depending on the from_end argument. However, you can provide a different function to use for truncation when you create the TextSplitter. Note that this class requires a _length_function to be defined in a parent or in this class itself. This function should take a string and return its length in units. The units could be anything (characters, words, sentences, etc.), depending on how you want to chunk your text."""
    text += ' '
    text += ' '.join(str(i) for i in range(900))
    chunks = splitter.split_text(text)
    print('\n\n\n-----------------------------------------------------\n\n\n'.join(chunks))