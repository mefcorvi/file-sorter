using ByteSizeLib;
using System.CommandLine;

namespace FileSorter
{
  internal static class SortCommand
  {
    public static Command GetCommand()
    {
      var fileNameOption = new Option<string>(
          "--file",
          getDefaultValue: () => "test.txt",
          description: "Name of the input file");
      fileNameOption.AddAlias("-f");

      var resultFileOption = new Option<string>(
          "--out",
          getDefaultValue: () => "result.txt",
          description: "Name of the output file");
      resultFileOption.AddAlias("-o");

      var chunkSizeOption = new Option<string>(
          "--chunk",
          getDefaultValue: () => "1gb",
          description: "Size of one chunk, it's recommended to use bigger value for bigger input file");
      chunkSizeOption.AddAlias("-c");

      var sortCommand = new Command("sort", "Sorts the specified file") {
        fileNameOption,
        resultFileOption,
        chunkSizeOption
      };

      sortCommand.SetHandler(
        async (string fileName, string resultFile, string chunkSize, CancellationToken token) =>
        {
          var sorter = new FileSorter(
            fileName,
            resultFile,
            (long)ByteSize.Parse(chunkSize).Bytes
          );

          await sorter.SortAsync(token);
        },
        fileNameOption,
        resultFileOption,
        chunkSizeOption
      );

      return sortCommand;
    }
  }
}
