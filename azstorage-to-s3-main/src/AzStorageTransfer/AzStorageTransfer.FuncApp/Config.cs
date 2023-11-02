﻿using Amazon;
using System;

namespace AzStorageTransfer.FuncApp
{
    public static class Config
    {
        public static string DataStorageConnection { get; } = Environment.GetEnvironmentVariable(nameof(DataStorageConnection));
        public static string ArchiveContainer { get; } = Environment.GetEnvironmentVariable(nameof(ArchiveContainer));
        public static string LiveContainer { get; } = Environment.GetEnvironmentVariable(nameof(LiveContainer));
        public static string ScheduledContainer { get; } = Environment.GetEnvironmentVariable(nameof(ScheduledContainer));
        public static string Prefix { get; } = Environment.GetEnvironmentVariable(nameof(Prefix));
        public static string FileExt { get; } = Environment.GetEnvironmentVariable(nameof(FileExt));

        public static class Aws
        {
            public static string AccessKey { get; } = Environment.GetEnvironmentVariable("AwsAccessKey");
            public static string SecretKey { get; } = Environment.GetEnvironmentVariable("AwsSecretKey");            
            public static string BucketName { get; } = Environment.GetEnvironmentVariable("AwsBucketName");
            public static RegionEndpoint Region { get; } = RegionEndpoint.GetBySystemName(Environment.GetEnvironmentVariable("AwsRegion") ?? RegionEndpoint.EUCentral1.SystemName);
        }
    }
}
