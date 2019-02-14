using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator
{
    public static class EnumerableExtensions
    {

        public static IEnumerable<IEnumerable<T>> Slice<T>(this IEnumerable<T> list, int size) {

            do
            {
                var top = list.Take(size);
                list = list.Skip(size);
                yield return list;

            } while (list.Any());
        }

    }
}
