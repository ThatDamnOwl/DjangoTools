Import-module PowerMySql -force

Function Invoke-DjangoPush
{
    param
    (
        $Server,
        $Creds,
        $Associations,
        $ExtraFields
    )

    Write-Verbose "Forming connection to $Server"
    $Connection = Get-MySqlConnection $Creds "$Server" "racktables"
    $Connection.open()
    foreach ($Association in @($Associations.Keys | sort))
    {
        $AssociationData = $Associations."$Association"
        Write-Host "Synchronising Table $Association"
        if ($AssociationData["type"] -eq "basic")
        {
            Write-Verbose "Relationship is basic executing push"
            $insertedCount = Invoke-MySqlNonQuery $Connection $AssociationData["push"]
            Write-Verbose "$insertedCount Rows pushed"
        }
        else {
            #Quick checks should return only one column with the unique IDs
            if ($AssociationData["quick_check"])
            {
                Write-Verbose "Quickly checking if there is any differences"
                $QuickCheck = Invoke-MySqlQuery $Connection $AssociationData["quick_check"] @(@{"Name"="UniqueID"})

                if ($QuickCheck.count -gt 0 -or ($QuickCheck -ne $null))
                {
                    Write-Verbose "There are differences"
                    $Run = $True
                    #Leaving this section for later
                }
                else {
                    Write-Verbose "There are no differences"
                    $Run = $False
                }
            }
            else
            {
                Write-Host "no quick check was declared for this database, it is advised to write one to avoid duplicate data"
                $Run = $True
            }

            if ($Run)
            {
                $OldTableName = $AssociationData["oldtable"]
                $NewTableName = $Association -replace "^\d*_"
                Write-Verbose "Synchronising Table $Association with old table $OldTableName"
                Write-Debug "Table Type is $($AssociationData['table_type'])"
                if ($AssociationData['table_type'] -eq "basic")
                {
                    Write-Debug "Getting Django Table data"
                    $DjangoTable = Get-MySqlTableDump $Connection "racktables_django" $NewTableName
                    Write-Debug "Getting Old Table Data"
                    $OldTable = Get-MySqlTableDump $Connection "racktables" $OldTableName
                }
                else 
                {
                    Write-Debug "Getting Django Table data"
                    Write-Debug "New table columns $($AssociationData['new_table_query']['columns'])"
                    $DjangoTable = Invoke-MySqlQuery $Connection $AssociationData['new_table_query']['query'] $AssociationData['new_table_query']['columns']
                    Write-Debug "Getting Old Table Data"
                    Write-Debug "Old table columns $($AssociationData['old_table_query']['columns'])"
                    $OldTable = Invoke-MySqlQuery $Connection $AssociationData['old_table_query']['query'] $AssociationData['old_table_query']['columns']
                }

                $OldKeyName = $AssociationData["oldkey"]
                $OldKeyNewName = $AssociationData["oldkey_newname"]
                #$OldTable
                if ($AssociationData["selfreference"])
                {
                    $SelfReferenceField = $AssociationData["oldselfreferencefield"]
                    Write-Debug "SelfRef = $SelfReferenceField"

                    $Inserting = $True
                    $layer = 0

                    While ($Inserting)
                    {
                        Write-Verbose "Pushing Layer $layer objects into database"

                        $InsertCandidates = $OldTable | where {
                            ($_."$SelfReferenceField" -in $DjangoTable.$OldKeyNewName) -xor `
                            ($layer -eq 0 -and ($_."$SelfReferenceField".ToString().length -eq 0))
                        }

                        Invoke-DjangoPushInsert $InsertCandidates $DjangoTable $NewTableName $OldTableName $AssociationData $Connection $ExtraFields

                        $DjangoTable = Get-MySqlTableDump $Connection "racktables_django" $NewTableName $Connection # $Server $Creds
                        #($OldTable | where {$_."$OldKeyName" -in $DjangoTable."$OldKeyNewName"}).count
                        #$DjangoTable."$OldKeyNewName"
                        #$OldTable.count
                        $Inserting = (($OldTable | where {$_."$OldKeyName" -in $DjangoTable."$OldKeyNewName"}).count -lt $OldTable.count) -and ($Layer -lt 4)
                        $layer++
                    }
                }
                else 
                {
                    Write-Debug "Straightforward Field maps"
                    Invoke-DjangoPushInsert $OldTable $DjangoTable $NewTableName $OldTableName $AssociationData $Connection $ExtraFields
                }
            }
        }
    }
    $Connection.close()
}

function Invoke-DjangoPushInsert
{
    param
    (
        $OldTable,
        $DjangoTable,
        $NewTableName,
        $OldTableName,
        $AssociationData,
        $Connection,
        $ExtraFields
    )
    $Inserted = @()

    Write-Verbose "There are $($OldTable.count) records in the old table"

    foreach ($Row in $OldTable)
    {
        $MatchedRecords = $DjangoTable | where {$_."$OldKeyNewName" -eq $Row."$OldKeyName"}
        $InsertedRecords = $Inserted | where {$_ -eq $Row."$OldKeyName"}
        if ($MatchedRecords -or $InsertedRecords)
        {
            write-Debug "$($Row.$OldKeyName) exists in $NewTableName"
        }
        else {
            $OldKey = $Row.$OldKeyName
            $Inserted += $OldKey
            write-Debug "$($Row.$OldKeyName) does not exist in $NewTableName, inserting it"
            $OldColumnData = @()
            $NewColumns = @()
            foreach ($Key in (@($AssociationData.keys) | where {$_ -notin $ExtraFields}))
            {
                Write-Debug "Old Column Data - $OldColumnData"
                if ($AssociationData[$Key] -eq "StringNull")
                {
                    $OldColumnData += ""
                }
                else {


                    if ($AssociationData[$Key].GetType().name -eq "string")
                    {
                        if ($AssociationData[$Key] -ne "NULL")
                        {
                            $OldColumnData += @($Row."$($AssociationData[$Key])" -replace "'","''")
                        }
                        else
                        {
                            $OldColumnData += "NULL"
                        }
                    }
                    elseif ($AssociationData[$Key].GetType().name -eq "Int32")
                    {
                        $OldColumnData += $AssociationData[$Key]
                    }
                    else 
                    {
                        $Arguments = @()
                        $DynamicKey = $AssociationData[$Key]

                        $NamedArgs = $DynamicKey["arguments"][0]
                        if ($NamedArgs['oldkey'])
                        {
                            Write-Debug "Lookup type is $($NamedArgs['lookuptype'])"
                            if ($NamedArgs['lookuptype'] -eq "Bunch")
                            {
                                Write-Debug "Getting bunch of values"
                                $SameID = $OldTable | where {$_.$OldKeyName -eq $OldKey}
                                $NamedArgs['lookup_value'] = $SameID
                            }
                            elseif ($NamedArgs['lookuptype'] -eq "Parse")
                            {  
                                Write-Debug "Getting parsed values"
                                $NamedArgs['lookup_value'] = $Row."$($NamedArgs['oldkey'])"
                                $NamedArgs['lookup_value'] = invoke-command -scriptblock $NamedArgs['scriptblock'] -argumentlist @($NamedArgs)
                            }
                            else {
                                Write-Debug "Getting single value"
                                $NamedArgs['lookup_value'] = $Row."$($NamedArgs['oldkey'])"
                            }

                            write-Debug "Value found = $($NamedArgs['lookup_value'])"
                        }
                        $NamedArgs += @{"Server" = $Server}
                        $NamedArgs += @{"Creds" = $Creds}

                        $Arguments += $NamedArgs

                        $tofs = $ofs
                        $ofs = ","
                        Write-Debug "OldColumnData Arguments - $Arguments"
                        $ofs = $tofs
                        $OldColumnDataTemp = invoke-command -scriptblock $DynamicKey["scriptblock"] -argumentlist $Arguments -verbose  
                        if ($DynamicKey["referencefield"])
                        {
                            if ($OldColumnDataTemp -eq $null)
                            {
                                $OldColumnDataTemp = "NULL"
                            }
                        }

                        $OldColumnData += $OldColumnDataTemp
                    }
                }
                $NewColumns += $Key
            }
            $tofs = $ofs
            $ofs = ","
            Write-Debug "$Arguments"
            Write-Debug "Connection;""racktables_django"";$NewTableName;$NewColumns;$OldColumnData"
            $ofs = $tofs
            $ignore = Invoke-MySqlInsert $Connection "racktables_django" $NewTableName $NewColumns $OldColumnData
        }
    }

    Write-Verbose "Inserted $($Inserted.count) records"
}

function Invoke-DjangoPull {
    param
    (
        $Server,
        $Creds
    )
}