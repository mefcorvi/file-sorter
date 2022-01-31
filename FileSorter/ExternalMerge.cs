using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

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
      Logger.Write("Merging files", $"file1={file1} file2={file2}");
      string resultFile = Path.GetTempFileName();
      var stopwatch = Stopwatch.StartNew();

      try
      {
        var resultSize = InternalSort(file1, file2, resultFile, token);
        Logger.Write("Files have been merged", $"file1={file1} file2={file2} output={resultFile} size={resultSize} duration={stopwatch.ElapsedMilliseconds}ms");
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

    private static long InternalSort(
      string file1,
      string file2,
      string outputFile,
      CancellationToken token)
    {
      List<byte> buffer1 = new List<byte>(1024);
      List<byte> buffer2 = new List<byte>(1024);

      int number1;
      int number2;

      using (var fs = OpenWriteStream(outputFile))
      using (var fs1 = OpenReadStream(file1))
      using (var fs2 = OpenReadStream(file2))
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
            // write current buffer
            WriteLine(fs, number1, buffer1);

            // copy rest of the file
            fs1.CopyTo(fs);
          }
          else if (buffer1.Count == 0)
          {
            // write current buffer
            WriteLine(fs, number2, buffer2);

            // copy rest of the file
            fs2.CopyTo(fs);
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

        return fs.Length;
      }
    }

    private static FileStream OpenWriteStream(string outputFile)
    {
      return new FileStream(
        outputFile,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None,
        1 << 16
      );
    }

    private static FileStream OpenReadStream(string file1)
    {
      return new FileStream(
        file1,
        FileMode.Open,
        FileAccess.Read,
        FileShare.None,
        1 << 16,
        FileOptions.SequentialScan);
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
        else if (b == newLine[0])
        {
          for (int i = 1; i < newLine.Length; i++)
          {
            s.ReadByte();
          }

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
