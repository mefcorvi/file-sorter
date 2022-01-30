using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FileSorter
{
  internal static class ExternalMerge
  {
    private static readonly byte[] NewLine = Encoding.ASCII.GetBytes(Environment.NewLine);

    public static string Sort(
      string file1,
      string file2,
      CancellationToken token)
    {
      Logger.Debug("Merging files", $"file1={file1} file2={file2}");
      string resultFile = Path.GetTempFileName();

      try
      {
        InternalSort(file1, file2, resultFile, token);
        Logger.Debug("Files have been merged", $"file1={file1} file2={file2} output={resultFile}");
      }
      catch
      {
        File.Delete(resultFile);
      }
      finally
      {
        File.Delete(file1);
        File.Delete(file2);
      }

      return resultFile;
    }

    private static void InternalSort(
      string file1,
      string file2,
      string outputFile,
      CancellationToken token)
    {
      List<byte> buffer1 = new List<byte>(1024);
      List<byte> buffer2 = new List<byte>(1024);

      int number1;
      int number2;

      using (var fs = new FileStream(outputFile, FileMode.Create, FileAccess.Write))
      using (var fs1 = new FileStream(file1, FileMode.Open, FileAccess.Read))
      using (var fs2 = new FileStream(file2, FileMode.Open, FileAccess.Read))
      {
        ReadLine(fs1, out number1, buffer1);
        ReadLine(fs2, out number2, buffer2);

        while (buffer1.Count > 0 || buffer2.Count > 0)
        {
          if (token.IsCancellationRequested)
          {
            break;
          }

          if (buffer2.Count == 0)
          {
            WriteLine(fs, number1, buffer1);
            ReadLine(fs1, out number1, buffer1);
          }
          else if (buffer1.Count == 0)
          {
            WriteLine(fs, number2, buffer2);
            ReadLine(fs2, out number2, buffer2);
          }
          else
          {
            int res = BufferCmp(buffer1, buffer2);

            if (res < 0
                || res == 0 && number1 == number2
                || res == 0 && number1 < number2)
            {
              WriteLine(fs, number1, buffer1);
              ReadLine(fs1, out number1, buffer1);
            }
            else
            {
              WriteLine(fs, number2, buffer2);
              ReadLine(fs2, out number2, buffer2);
            }
          }
        }
      }
    }

    private static void WriteLine(Stream s, int number, List<byte> buffer)
    {
      s.Write(Encoding.ASCII.GetBytes(number.ToString()));
      s.WriteByte(46);
      s.WriteByte(32);
      s.Write(CollectionsMarshal.AsSpan(buffer));
      s.Write(NewLine);
      buffer.Clear();
    }

    private static void ReadLine(Stream s, out int number, List<byte> buffer)
    {
      number = 0;
      bool isNumber = true;
      var newLine = NewLine;

      while (true)
      {
        int b = s.ReadByte();

        if (b == -1)
        {
          return;
        }

        if (isNumber && b < 58 && b >= 48)
        {
          number *= 10;
          number += b - 48;
        }
        else if (b == 46 && isNumber)
        {
          // we're reading a number but current char is a separator
          isNumber = false;
          s.ReadByte();
        }
        else if (b == 13 || b == 10)
        {
          if (!isNumber)
          {
            return;
          }
        }
        else
        {
          buffer.Add((byte)b);
        }
      }
    }

    private static int BufferCmp(List<byte> x, List<byte> y)
    {
      int compareLength = Math.Min(x.Count, y.Count);

      for (int i = 0; i < compareLength; i++)
      {
        if (x[i] != y[i])
        {
          return x[i] > y[i] ? 1 : -1;
        }
      }

      if (x.Count > y.Count)
      {
        return 1;
      }

      if (x.Count < y.Count)
      {
        return -1;
      }

      return 0;
    }
  }
}
