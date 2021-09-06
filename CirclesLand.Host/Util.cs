using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using DotNetHost = Microsoft.Extensions.Hosting.Host;

namespace CirclesLand.Host
{
    static class Util
    {
        public static void PrintException(object e)
        {
            Console.WriteLine($"An exception occurred:");

            var currentException = e as Exception;
            var indention = 0;

            while (currentException != null)
            {
                var indentionStr = new string(' ', indention);
                Console.WriteLine(indentionStr + currentException.Message);
                Console.WriteLine(indentionStr + currentException.StackTrace);

                if (currentException.InnerException != null)
                {
                    indention += 2;
                }

                currentException = currentException.InnerException;
            }
        }
    }
}