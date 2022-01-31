using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace FileSorter
{
  internal sealed class FileSorter
  {
    private readonly long chunkSize;
    private readonly string fileName;
    private readonly string resultFile;
    private readonly byte[] newLine;

    public FileSorter(string fileName, string resultFile, long chunkSize)
    {
      this.newLine = Encoding.ASCII.GetBytes(Environment.NewLine);
      this.fileName = fileName;
      this.resultFile = resultFile;
      this.chunkSize = chunkSize;
    }

    public async ValueTask SortAsync(CancellationToken token = default(CancellationToken))
    {
      Logger.Write("Sorting the file", $"input={this.fileName} output={this.resultFile} chunkSize={this.chunkSize}");

      var sw = new Stopwatch();
      sw.Start();

      var sortedFiles = new ConcurrentStack<string>();
      var sortTasks = new List<Task>();

      long inputFileSize = new FileInfo(fileName).Length;

      using (var file = MemoryMappedFile.CreateFromFile(this.fileName, FileMode.Open, null, 0, MemoryMappedFileAccess.Read))
      {
        var items = new List<FileItem>[256];

        for (int i = 0; i < items.Length; i++)
        {
          items[i] = new List<FileItem>(1 << 16);
        }

        long offset = 0;

        while (!token.IsCancellationRequested && offset < inputFileSize)
        {
          using (var accessor = file.CreateViewAccessor(offset, 0, MemoryMappedFileAccess.Read))
          {
            if (accessor.Capacity == 0)
            {
              break;
            }

            var comparer = new OffsetComparer(accessor);

            Logger.Write("Reading a chunk...", $"offset={offset}");
            long chunkOffset = 0;
            long linesRead = 0;

            ReadLines(accessor, items, out chunkOffset, out linesRead, token);
            offset += chunkOffset;

            if (linesRead > 0)
            {
              Logger.Write("Sorting items...", $"offset={offset}");
              items.Where(x => x != null).AsParallel().ForAll(x => x.Sort(comparer));

              Logger.Write("Items have been sorted", $"offset={offset}");
              var fileName = WriteSortedLists(accessor, items, token);

              if (sortedFiles.TryPop(out var prevFile))
              {
                var sortTask = Task.Factory.StartNew(() =>
                {
                  sortedFiles.Push(ExternalMerge.Sort(fileName, prevFile, token));
                }, TaskCreationOptions.LongRunning);
                sortTasks.Add(sortTask);
              }
              else
              {
                sortedFiles.Push(fileName);
              }
            }

            for (int i = 0; i < items.Length; i++)
            {
              items[i].Clear();
            }
          }
        }
      }

      await Task.WhenAll(sortTasks);

      if (!token.IsCancellationRequested)
      {
        var resultFile = sortedFiles
          .AsParallel()
          .WithCancellation(token)
          .Aggregate((current, next) => ExternalMerge.Sort(current, next, token));

        File.Move(resultFile, this.resultFile, true);
        Logger.Write($"Result file has been generated", $"fileName={this.resultFile}");
        sortedFiles.Clear();
      }

      DeleteTempFiles(sortedFiles);

      Logger.Write($"Finished in {sw.ElapsedMilliseconds}ms");
    }

    private static void DeleteTempFiles(IEnumerable<string> sortedFiles)
    {
      foreach (var item in sortedFiles)
      {
        try
        {
          File.Delete(item);
          Logger.Write("Temp file has been deleted", $"fileName={item}");
        }
        catch (Exception err)
        {
          Logger.Write("Failed to delete temp file", $"fileName={item} error={err}");
        }
      }
    }

    private void ReadLines(
      MemoryMappedViewAccessor accessor,
      List<FileItem>[] items,
      out long offset,
      out long linesRead,
      CancellationToken token)
    {
      long chunkSize = this.chunkSize;
      bool isNumber = true;
      uint prefix = 0;
      long itemOffset = -1;
      ushort bucket = 0;
      int number = 0;
      linesRead = 0;

      for (offset = 0; offset < accessor.Capacity; offset++)
      {
        byte b = accessor.ReadByte(offset);

        if (isNumber && b < 58 && b >= 48)
        {
          number *= 10;
          number += b - 48;
        }
        else if (isNumber && b == 46)
        {
          // we're reading a number but current char is a separator
          isNumber = false;
          itemOffset = offset + 2;

          offset++;
        }
        else if (b == this.newLine[0])
        {
          // we've reached EOL so add current item to the correct bucket
          // and continue reading
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

          if (offset >= chunkSize)
          {
            break;
          }

          if (token.IsCancellationRequested)
          {
            break;
          }
        }
        else if (!isNumber && offset < itemOffset + 5)
        {
          if (offset < itemOffset + 1)
          {
            // reading bucket
            bucket = b;
          }
          else
          {
            // reading prefix part
            prefix = prefix << 8;
            prefix += b;
          }
        }
      }

      Logger.Write("Chunk has been read", $"offset={offset} linesCount={linesRead}");
    }

    /// <summary>
    /// Writes sorted chunks into a file and returns path to this file.
    /// </summary>
    private string WriteSortedLists(
      MemoryMappedViewAccessor accessor,
      List<FileItem>[] items,
      CancellationToken token)
    {
      string fileName = Path.GetTempFileName();
      long linesWritten = 0;

      using (var fs = new FileStream(
        fileName,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None,
        1 << 16,
        false))
      {
        for (int i = 0; i < items.Length; i++)
        {
          if (token.IsCancellationRequested)
          {
            break;
          }

          if (items[i] != null)
          {
            for (int j = 0; j < items[i].Count; j++)
            {
              var item = items[i][j];
              var number = item.Number.ToString();

              for (int k = 0; k < number.Length; k++)
              {
                fs.WriteByte((byte)number[k]);
              }

              fs.WriteByte(46);
              fs.WriteByte(32);

              var itemOffset2 = item.Offset > 0 ? item.Offset - 5 : -1;
              byte b = 0;

              if (itemOffset2 == -1)
              {
                fs.WriteByte((byte)i);

                if (item.Prefix > 0)
                {
                  var prefixBytes = BitConverter.GetBytes(item.Prefix);

                  for (int k = 0; k < prefixBytes.Length; k++)
                  {
                    if (prefixBytes[k] > 0)
                    {
                      fs.WriteByte(prefixBytes[k]);
                    }
                  }
                }

                linesWritten++;
                fs.Write(this.newLine);
                continue;
              }

              while (itemOffset2 < accessor.Capacity)
              {
                b = accessor.ReadByte(itemOffset2);
                itemOffset2++;

                if (b == this.newLine[0])
                {
                  linesWritten++;
                  fs.Write(this.newLine);
                  break;
                }

                fs.WriteByte(b);
              }
            }
          }
        }
      }

      Logger.Write("Temporary files have been created", $"linesWritten={linesWritten} fileName={fileName}");
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
