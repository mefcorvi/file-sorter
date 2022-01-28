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

            long offset = 0;

            using (var file = MemoryMappedFile.CreateFromFile(this.fileName, FileMode.Open))
            using (var accessor = file.CreateViewAccessor(offset, 0, MemoryMappedFileAccess.Read))
            {
                var comparer = new OffsetComparer(accessor);
                var items = new List<FileItem>[256];
                bool isNumber = true;
                uint prefix = 0;
                long itemOffset = -1;
                ushort bucket = 0;
                int number = 0;

                for (int i = 0; i < items.Length; i++)
                {
                    items[i] = new List<FileItem>(1 << 16);
                }

                for (long i = 0; i < accessor.Capacity; i++)
                {
                    byte b = accessor.ReadByte(i);

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
                        itemOffset = i + 2;

                        i++;
                        continue;
                    }

                    // reading a EOL
                    if (b == this.newLine[0])
                    {
                        if (i < itemOffset + 5)
                        {
                            itemOffset = -1;
                        } else
                        {
                            itemOffset += 5;
                        }

                        items[bucket].Add(new FileItem(number, prefix, itemOffset));
                        prefix = 0;
                        bucket = 0;
                        number = 0;
                        isNumber = true;


                        i += this.newLine.Length - 1;
                        continue;
                    }

                    if (!isNumber)
                    {
                        if (i < itemOffset + 1)
                        {
                            bucket = b;
                        }
                        else if (i < itemOffset + 5)
                        {
                            prefix = prefix << 8;
                            prefix += b;
                        }
                    }
                }

                items.Where(x => x != null).AsParallel().ForAll(x => x.Sort(comparer));

                int linesWritten = 0;

                using (var fs = new FileStream("result.txt", FileMode.Create))
                using (var bs = new BufferedStream(fs))
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

                                    bs.Write(this.newLine);
                                    linesWritten++;
                                    continue;
                                }

                                while (itemOffset2 < accessor.Capacity)
                                {
                                    b = accessor.ReadByte(itemOffset2);
                                    itemOffset2++;

                                    if (b == 13)
                                    {
                                        bs.Write(this.newLine);
                                        break;
                                    }

                                    bs.WriteByte(b);
                                }

                                linesWritten++;
                            }
                        }
                    }
                }

                Console.WriteLine($"Finished in {sw.ElapsedMilliseconds}ms, {linesWritten} lines written");
            }
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
