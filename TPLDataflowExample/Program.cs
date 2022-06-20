// Demonstrates how to create a basic dataflow pipeline.
// This program downloads the book "The Iliad of Homer" by Homer from the Web
// and finds all reversed words that appear in that book.

using System.Threading.Tasks.Dataflow;

// Demonstrates how to create a basic dataflow pipeline.
// This program downloads the book "The Iliad of Homer" by Homer from the Web
// and finds all reversed words that appear in that book.
static class Program
{
  static void Main()
  {
    // Downloads the requested resource as a string.
    var downloadString = new TransformBlock<string, string>(async uri =>
    {
      Console.WriteLine("Downloading '{0}'...", uri);

      return await new HttpClient(new HttpClientHandler { AutomaticDecompression = System.Net.DecompressionMethods.GZip }).GetStringAsync(uri);
    });

    // Separates the specified text into an array of words.
    var createWordList = new TransformBlock<string, string[]>(text =>
    {
      Console.WriteLine("Creating word list...");

      // Remove common punctuation by replacing all non-letter characters
      // with a space character.
      char[] tokens = text.Select(c => char.IsLetter(c) ? c : ' ').ToArray();
      text = new string(tokens);

      // Separate the text into an array of words.
      var wordList = text.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

      Console.WriteLine($"\tCreated {wordList.Length} words");

      return wordList;
    });

    // Removes short words and duplicates.
    var filterWordList = new TransformBlock<string[], string[]>(words =>
    {
      Console.WriteLine("Filtering word list ( > 3 chars and ignore duplicates) ...");

      var filteredWords = words
         .Where(word => word.Length > 3)
         .Distinct()
         .ToArray();

      Console.WriteLine($"\tFiltered down to {filteredWords.Length} words");

      return filteredWords;
    });

    // Finds all words in the specified collection whose reverse also
    // exists in the collection.
    var findReversedWords = new TransformManyBlock<string[], string>(words =>
    {
      Console.WriteLine("Finding reversed words...");

      var wordsSet = new HashSet<string>(words);
      var reversedWords = from word in words.AsParallel()
                          let reverse = new string(word.Reverse().ToArray())
                          where word != reverse && wordsSet.Contains(reverse)
                          select word;

      Console.WriteLine($"\tFound {reversedWords.Count()} reversed words");

      return reversedWords;
    });

    // Prints the provided reversed words to the console.
    var printReversedWords = new ActionBlock<string>(reversedWord =>
    {
      Console.WriteLine("Found reversed words {0}/{1}",
         reversedWord, new string(reversedWord.Reverse().ToArray()));
    });

    //
    // Connect the dataflow blocks to form a pipeline.
    //

    var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

    downloadString.LinkTo(createWordList, linkOptions);
    createWordList.LinkTo(filterWordList, linkOptions);
    filterWordList.LinkTo(findReversedWords, linkOptions);
    findReversedWords.LinkTo(printReversedWords, linkOptions);

    // Process "The Iliad of Homer" by Homer.
    downloadString.Post("http://www.gutenberg.org/cache/epub/16452/pg16452.txt");

    // Mark the head of the pipeline as complete.
    downloadString.Complete();

    // Wait for the last block in the pipeline to process all messages.
    printReversedWords.Completion.Wait();

    Console.ReadKey();
  }
}

