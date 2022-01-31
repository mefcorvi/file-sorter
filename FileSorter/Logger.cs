using System;
using System.Diagnostics;

public static class Logger
{
  private static readonly Stopwatch Stopwatch = Stopwatch.StartNew();

  public static void Write(string msg, string? param = null)
  {
    string addon = param != null ? $" | {param}" : "";

    Console.WriteLine($"{Logger.Stopwatch.Elapsed.ToString("g")} | {msg}{addon}");
  }
}
