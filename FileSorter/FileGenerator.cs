using System.Diagnostics;

namespace FileSorter
{
  internal sealed class FileGenerator
  {
    private readonly string fileName;
    private readonly long fileSize;
    private readonly int maxLineSize;
    private readonly Random random;
    private readonly byte[] newLine;
    private readonly byte minLineSize;

    public FileGenerator(string fileName, long fileSize, int maxLineSize)
    {
      // 10 characters for the number part + 2 chars for separator +
      // 2 chars for newline + 1 char for a string part
      this.minLineSize = 15;

      if (maxLineSize < this.minLineSize)
      {
        throw new ArgumentOutOfRangeException(
            $"Line size could not be lesser than {this.minLineSize} characters");
      }

      this.newLine = System.Text.Encoding.ASCII.GetBytes(Environment.NewLine);
      this.random = new Random();
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.maxLineSize = maxLineSize;
    }

    public async ValueTask GenerateAsync(
        CancellationToken cancellationToken = default(CancellationToken))
    {
      Console.WriteLine($"Generating a new test file. " +
          $"Target file name is \"{this.fileName}\", " +
          $"target size is {this.fileSize} bytes");

      var buffer = new byte[this.maxLineSize];
      int bufferSize = 0;
      long currentSize = 0;
      long linesCount = 0;

      var stopwatch = new Stopwatch();
      stopwatch.Start();

      using (var stream = new FileStream(
        this.fileName,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None,
        1 << 16))
      {
        while (currentSize < this.fileSize && !cancellationToken.IsCancellationRequested)
        {
          this.FillBuffer(buffer, ref bufferSize);
          await stream.WriteAsync(buffer, 0, bufferSize);
          currentSize += bufferSize;
          linesCount++;
        }
      }

      Console.WriteLine(
          $"Finished in {stopwatch.ElapsedMilliseconds}ms, " +
          $"{currentSize / Math.Max(stopwatch.ElapsedMilliseconds, 1)} bytes/ms, " +
          $"{linesCount} lines written");
    }

    private void FillBuffer(Span<byte> buffer, ref int size)
    {
      string number = this.random.Next().ToString();

      for (size = 0; size < number.Length; size++)
      {
        buffer[size] = (byte)number[size];
      }

      // Add separator ". "
      buffer[size++] = 46;
      buffer[size++] = 32;

      int stringSize = this.random.Next(size + 1, this.maxLineSize - this.newLine.Length);

      while (size < stringSize)
      {
        // write capitalized letters
        buffer[size++] = (byte)this.random.Next(65, 91);
      }

      for (int i = 0; i < this.newLine.Length; i++)
      {
        buffer[size++] = this.newLine[i];
      }
    }
  }
}
