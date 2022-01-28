using System;

public static class Logger
{
  private static DateTime LastTime = DateTime.Now;

  public static void Debug(string msg, string? param = null)
  {
    var now = DateTime.Now;
    double timeDiff = Math.Max((now - Logger.LastTime).TotalMilliseconds, 0);
    Logger.LastTime = now;

    string addon = param != null ? $" | {param}" : "";

    Console.WriteLine($"DEBUG | {timeDiff}ms | {msg}{addon}");
  }
}
