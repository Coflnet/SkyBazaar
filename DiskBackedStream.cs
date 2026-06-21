using System;
using System.IO;

namespace Coflnet.Sky.Core
{
    /// <summary>
    /// Write-only stream that buffers up to <see cref="Threshold"/> bytes in memory,
    /// then transparently spills to a temporary file under <c>/tmp</c>.
    /// The temp file is deleted on dispose via <see cref="FileOptions.DeleteOnClose"/>.
    ///
    /// Use case: gzip/brotli serialisation of large auction/bazaar payloads that
    /// would otherwise OOM a MemoryStream during long time-range exports.
    /// Default operations stay entirely in-memory.
    /// </summary>
    public class DiskBackedStream : Stream
    {
        public const long DefaultThreshold = 5 * 1024 * 1024; // 5 MiB

        private readonly long _threshold;
        private MemoryStream _memory;
        private FileStream _file;
        private bool _disposed;

        public long Threshold => _threshold;

        public DiskBackedStream(long threshold = DefaultThreshold)
        {
            _threshold = threshold;
            _memory = new MemoryStream();
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => true;
        public override long Length => _file != null ? _file.Length : _memory.Length;

        public override long Position
        {
            get => _file != null ? _file.Position : _memory.Position;
            set
            {
                if (_file != null) _file.Position = value;
                else _memory.Position = value;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            if (_file != null)
            {
                _file.Write(buffer, offset, count);
                return;
            }

            if (_memory.Length + count > _threshold)
                SpillToDisk();

            if (_file != null)
                _file.Write(buffer, offset, count);
            else
                _memory.Write(buffer, offset, count);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            return (_file ?? (Stream)_memory).Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            ThrowIfDisposed();
            return (_file ?? (Stream)_memory).Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            ThrowIfDisposed();
            (_file ?? (Stream)_memory).SetLength(value);
        }

        public override void Flush()
        {
            (_file ?? (Stream)_memory)?.Flush();
        }

        /// <summary>Returns the complete content as a byte array.</summary>
        public byte[] ToArray()
        {
            ThrowIfDisposed();
            Flush();
            if (_file != null)
            {
                var pos = _file.Position;
                _file.Position = 0;
                var result = new byte[_file.Length];
                _file.Read(result, 0, result.Length);
                _file.Position = pos;
                return result;
            }
            return _memory.ToArray();
        }

        private void SpillToDisk()
        {
            var tempPath = Path.Combine(Path.GetTempPath(), $"blob-{Guid.NewGuid():N}.tmp");
            _file = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite,
                FileShare.None, 4096, FileOptions.DeleteOnClose);
            _memory.Position = 0;
            _memory.CopyTo(_file);
            _memory.Dispose();
            _memory = null;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(DiskBackedStream));
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _memory?.Dispose();
                _file?.Dispose();
                _disposed = true;
            }
            base.Dispose(disposing);
        }
    }
}
