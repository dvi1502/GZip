using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Xml.Linq;
using Microsoft.SqlServer.Server;


namespace GZip
{

    //компонента для сжатия/распаковки двоичных данных по алгоритму zlib(RFC 1950), deflate(RFC 1951) и gzip(RFC 1952)

    public partial class GZip
    {

        //> "zlib" или 0 - алгоритм zlib(RFC 1950);
        //> "deflate" или 1 - алгоритм deflate(RFC 1951);
        //> "gzip" или 2 - алгоритм gzip(RFC 1952);

        [SqlFunction(Name = "Compress", IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlBytes CompressBinData(SqlBytes data, int compressionAlgorithm = 1)
        {
            using (MemoryStream originalFileStream = new MemoryStream())
            {
                using (MemoryStream compressedFileStream = new MemoryStream((byte[])data.Value))
                {

                    switch (compressionAlgorithm)
                    {
                        case 1:
                            {
                                using (DeflateStream compressionStream = new DeflateStream(compressedFileStream, CompressionMode.Compress))
                                {
                                    originalFileStream.CopyTo(compressionStream);
                                    return new SqlBytes(originalFileStream.ToArray());
                                }
                            }

                        case 2:
                            {
                                using (GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress))
                                {
                                    originalFileStream.CopyTo(compressionStream);
                                    return new SqlBytes(originalFileStream.ToArray());
                                }
                            }
                        default:
                            return new SqlBytes();
                    }

                }
            }

        }

        [SqlFunction(Name = "Uncompress", IsDeterministic = true, IsPrecise = true, DataAccess = DataAccessKind.None)]
        public static SqlBytes DeCompressBinData(SqlBytes data, int compressionAlgorithm = 1)
        {
            using (MemoryStream originalFileStream = new MemoryStream( (byte[])data.Value) )
            {
                using (MemoryStream decompressedFileStream = new MemoryStream())
                {
                    switch (compressionAlgorithm)
                    {
                        case 1:
                            using (DeflateStream decompressionStream = new DeflateStream(originalFileStream, CompressionMode.Decompress))
                            {
                                decompressionStream.CopyTo(decompressedFileStream);
                                return new SqlBytes(decompressedFileStream.ToArray());
                            }

                        case 2:
                            using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                            {
                                decompressionStream.CopyTo(decompressedFileStream);
                                return new SqlBytes(decompressedFileStream.ToArray());
                            }
                        default:
                            return new SqlBytes();
                    }
                }
            }
        }


    }
}
