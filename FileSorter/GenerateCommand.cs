using System.CommandLine;

namespace FileSorter
{
    internal static class GenerateCommand
    {
        public static Command GetCommand()
        {
            var fileSizeOption = new Option<long>(
                "--size",
                getDefaultValue: () => 1 << 30,
                description: "Size of the target file to generate");
            fileSizeOption.AddAlias("-s");

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
                fileSizeOption,
                fileNameOption,
                maxLineSizeOption,
            };

            generateCommand.SetHandler(
                async (long fileSize, string fileName, int maxLineSize, CancellationToken token) =>
                {
                    var generator = new FileGenerator(fileName, fileSize, maxLineSize);
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
