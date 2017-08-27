using System;
using System.Collections.Generic;
using System.Linq;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public static class LiteDbExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="coll"></param>
        /// <typeparam name="T"></typeparam>
        public static void DeleteOne<T>(this LiteCollection<T> coll)
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="coll"></param>
        public static void FindOneAndUpdate<T>(this LiteCollection<T> coll)
        {
            
        }
        
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static LiteCollection<TResult> OfType<TResult>(this LiteCollection<TResult> source)
        {
            throw  new NotImplementedException();
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="items"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static IEnumerable<BsonValue> ToBsonValueEnumerable<T>(this IEnumerable<T> items)
        {
            return items.Select(item => new BsonValue(item));
        }
    }
}