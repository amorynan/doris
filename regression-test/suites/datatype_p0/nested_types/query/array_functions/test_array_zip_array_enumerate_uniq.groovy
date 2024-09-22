// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_array_zip_array_enumerate_uniq", "p0") {


    sql "set enable_nereids_planner=false;"
//     ========== array-zip ==========
//     wrong case
    test {
        sql """
               SELECT array_zip();
                """
        exception("errCode = 2")
    }

    test {
        sql """
               SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']);
                """
        exception("errCode = 2")
    }

    // ============= array_enumerate_uniq =========
    qt_sql "SELECT 'array_enumerate_uniq';"
    sql """ set enable_fold_constant_by_be=true; """ // if we enable_fold_constant_by_be=false, will meet cast error`
    order_qt_old_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """

    order_qt_old_sql """SELECT array_enumerate_uniq(
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666]);"""
    order_qt_old_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
    //order_qt_sql """ SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""

    // nereids

    // nereid not support array_enumerate_uniq
    // ============= array_enumerate_uniq =========
//    qt_sql "SELECT 'array_enumerate_uniq';"
//    order_qt_nereid_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """
//
//    order_qt_nereid_sql """SELECT array_enumerate_uniq(
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666],
//            [111111, 222222, 333333],
//            [444444, 555555, 666666]);"""
//    order_qt_nereid_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
//    //order_qt_sql """ SELECT max(array_join(arr)) FROM (SELECT array_enumerate_uniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""


}
