package org.example.domain

import org.example.NoArgEntity

@NoArgEntity
class Employee(
    var id:Int,
    var name:String,
    var department: Department
)