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
          description: "Name of the target file");
      fileNameOption.AddAlias("-f");

      var sortCommand = new Command("sort", "Sorts the specified file") {
        fileNameOption,
      };

      sortCommand.SetHandler(
        async (string fileName, CancellationToken token) =>
        {
          var sorter = new FileSorter(fileName);
          await sorter.SortAsync(token);
        },
        fileNameOption
      );

      return sortCommand;
    }
  }
}
