package org.example.domain

import org.example.NoArgEntity

@NoArgEntity
class Department(
    var deptId:Int,
    var deptName:String,
    var deptHead:String
)
