using FileSorter;
using System.CommandLine;

var rootCommand = new RootCommand("Application for sorting really big files");

rootCommand.AddCommand(GenerateCommand.GetCommand());
rootCommand.AddCommand(SortCommand.GetCommand());

rootCommand.Invoke(args);