using ByteSizeLib;
using System.CommandLine;

namespace FileSorter
{
  internal static class GenerateCommand
  {
    public static Command GetCommand()
    {
      var fileSizeOption = new Argument<string>(
          "size",
          getDefaultValue: () => "1gb",
          description: "Size of the target file to generate");

      var fileNameOption = new Option<string>(
          "--file",
          getDefaultValue: () => "test.txt",
          description: "Name of the target file");
      fileNameOption.AddAlias("-f");

      var maxLineSizeOption = new Option<int>(
          "--line-size",
          getDefaultValue: () => 256,
          description: "Size of the lines");
      maxLineSizeOption.AddAlias("-l");

      var generateCommand = new Command("generate", "Generates a new test file") {
        fileNameOption,
        maxLineSizeOption,
      };
      generateCommand.AddArgument(fileSizeOption);

      generateCommand.SetHandler(
          async (string fileSize, string fileName, int maxLineSize, CancellationToken token) =>
          {
            var generator = new FileGenerator(
                    fileName,
                    (long)ByteSize.Parse(fileSize).Bytes,
                    maxLineSize);
            await generator.GenerateAsync(token);
          },
          fileSizeOption,
          fileNameOption,
          maxLineSizeOption
      );

      return generateCommand;
    }
  }
}
