using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace FileSorter
{
  internal sealed class FileSorter
  {
    private readonly string fileName;
    private readonly byte[] newLine;

    public FileSorter(string fileName)
    {
      this.newLine = System.Text.Encoding.ASCII.GetBytes(Environment.NewLine);
      this.fileName = fileName;
    }

    public async ValueTask SortAsync(CancellationToken token = default(CancellationToken))
    {
      var sw = new Stopwatch();
      sw.Start();

      var sortedFiles = new List<string>();

      using (var file = MemoryMappedFile.CreateFromFile(this.fileName, FileMode.Open))
      {
        var items = new List<FileItem>[256];

        for (int i = 0; i < items.Length; i++)
        {
          items[i] = new List<FileItem>(1 << 16);
        }

        long offset = 0;

        while (!token.IsCancellationRequested)
        {
          using (var accessor = file.CreateViewAccessor(offset, 0, MemoryMappedFileAccess.Read))
          {
            if (accessor.Capacity == 0)
            {
              break;
            }

            var comparer = new OffsetComparer(accessor);

            Logger.Debug("Reading batch", $"offset={offset}");
            long batchOffset = 0;

            ReadLines(accessor, items, out batchOffset, token);
            offset += batchOffset;

            Logger.Debug("Sorting lines...", $"offset={offset}");
            items.Where(x => x != null).AsParallel().ForAll(x => x.Sort(comparer));

            Logger.Debug("Items has been sorted, writing to the temporary file...", $"offset={offset}");
            sortedFiles.Add(WriteSortedLists(accessor, items));

            for (int i = 0; i < items.Length; i++)
            {
              items[i].Clear();
            }
          }
        }
      }

      DeleteTempFiles(sortedFiles);

      Console.WriteLine($"Finished in {sw.ElapsedMilliseconds}ms");
    }

    private static void DeleteTempFiles(List<string> sortedFiles)
    {
      foreach (var item in sortedFiles)
      {
        try
        {
          File.Delete(item);
          Logger.Debug("Temp file has been deleted", $"fileName={item}");
        }
        catch (Exception err)
        {
          Logger.Debug("Failed to delete temp file", $"fileName={item} error={err}");
        }
      }
    }

    private void ReadLines(
      MemoryMappedViewAccessor accessor,
      List<FileItem>[] items,
      out long offset,
      CancellationToken token)
    {
      bool isNumber = true;
      uint prefix = 0;
      long itemOffset = -1;
      ushort bucket = 0;
      int number = 0;
      long linesRead = 0;

      for (offset = 0; offset < accessor.Capacity; offset++)
      {
        byte b = accessor.ReadByte(offset);

        if (isNumber && b >= 48 && b < 58)
        {
          number *= 10;
          number += b - 48;
        }

        // we're reading a number but current char is a separator
        if (b == 46 && isNumber)
        {
          if (token.IsCancellationRequested)
          {
            break;
          }

          isNumber = false;
          itemOffset = offset + 2;

          offset++;
          continue;
        }

        // reading a EOL
        if (b == this.newLine[0])
        {
          if (offset < itemOffset + 5)
          {
            itemOffset = -1;
          }
          else
          {
            itemOffset += 5;
          }

          items[bucket].Add(new FileItem(number, prefix, itemOffset));
          prefix = 0;
          bucket = 0;
          number = 0;
          isNumber = true;

          offset += this.newLine.Length - 1;

          linesRead++;
          if (offset > 1 << 30)
          {
            break;
          }


          continue;
        }

        if (!isNumber)
        {
          if (offset < itemOffset + 1)
          {
            bucket = b;
          }
          else if (offset < itemOffset + 5)
          {
            prefix = prefix << 8;
            prefix += b;
          }
        }
      }

      Logger.Debug("Batch has been read", $"offset={offset} linesCount={linesRead}");
    }

    /// <summary>
    /// Writes sorted chunks into a file and returns path to this file.
    /// </summary>
    private string WriteSortedLists(MemoryMappedViewAccessor accessor, List<FileItem>[] items)
    {
      string fileName = Path.GetTempFileName();
      long linesWritten = 0;

      using (var fs = new FileStream(fileName, FileMode.Create))
      using (var bs = new BufferedStream(fs, 1 << 16))
      {
        for (int i = 0; i < items.Length; i++)
        {
          if (items[i] != null)
          {
            for (int j = 0; j < items[i].Count; j++)
            {
              var item = items[i][j];
              bs.Write(Encoding.ASCII.GetBytes(item.Number.ToString()));
              bs.WriteByte(46);
              bs.WriteByte(32);

              var itemOffset2 = item.Offset > 0 ? item.Offset - 5 : -1;
              byte b = 0;

              if (itemOffset2 == -1)
              {
                bs.WriteByte((byte)i);

                if (item.Prefix > 0)
                {
                  var prefixBytes = BitConverter.GetBytes(item.Prefix);

                  for (int k = 0; k < prefixBytes.Length; k++)
                  {
                    if (prefixBytes[k] > 0)
                    {
                      bs.WriteByte(prefixBytes[k]);
                    }
                  }
                }

                linesWritten++;
                bs.Write(this.newLine);
                continue;
              }

              while (itemOffset2 < accessor.Capacity)
              {
                b = accessor.ReadByte(itemOffset2);
                itemOffset2++;

                if (b == 13)
                {
                  linesWritten++;
                  bs.Write(this.newLine);
                  break;
                }

                bs.WriteByte(b);
              }
            }
          }
        }
      }

      Logger.Debug("Temporary files has been created", $"linesWritten={linesWritten} fileName={fileName}");
      return fileName;
    }
  }

  readonly struct FileItem
  {
    public readonly int Number;

    public readonly uint Prefix;

    public readonly long Offset;

    public FileItem(int number, uint prefix, long offset)
    {
      Number = number;
      Prefix = prefix;
      Offset = offset;
    }
  }

  sealed class OffsetComparer : IComparer<FileItem>
  {
    private readonly MemoryMappedViewAccessor accessor;

    public OffsetComparer(MemoryMappedViewAccessor accessor)
    {
      this.accessor = accessor;
    }

    public int Compare(FileItem x, FileItem y)
    {
      if (x.Prefix != y.Prefix)
      {
        return (int)x.Prefix - (int)y.Prefix;
      }

      if (x.Offset == -1 && y.Offset == -1)
      {
        return x.Number - y.Number;
      }

      if (x.Offset == -1)
      {
        return -1;
      }

      if (y.Offset == 1)
      {
        return 1;
      }

      long offset = 0;

      while (x.Offset + offset < accessor.Capacity && y.Offset + offset < accessor.Capacity)
      {
        var b1 = accessor.ReadByte(offset + x.Offset);
        var b2 = accessor.ReadByte(offset + y.Offset);

        if (b1 == 13 && b2 == 13)
        {
          break;
        }

        if (b1 == 13)
        {
          return 1;
        }

        if (b2 == 13)
        {
          return -1;
        }

        if (b1 == b2)
        {
          offset++;
          continue;
        }

        return b1 - b2;
      }

      return x.Number - y.Number;
    }
  }
}
