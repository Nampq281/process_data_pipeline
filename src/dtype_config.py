from polars import Float64, List, Struct, String

# Contract_Instalments_GrantedContract
# Contract_Instalments_NotGrantedContract
# Contract_NonInstalments_GrantedContract
# Contract_NonInstalments_NotGrantedContract
# Contract_Cards_GrantedContract
# Contract_Cards_NotGrantedContract
# CurrencySummary_CurrencyPercentage

##---------------------------Contract level parse scheme for nested data---------------------------
Contract_Instalments_GrantedContract = 	List(Struct({
                                                  "CommonData": Struct({
                                                    "CBContractCode": String,
                                                    "FIContractCode": String,
                                                    "Currency": String,
                                                    "ReferenceNumber": String,
                                                    "Role": String,
                                                    "EncryptedFICode": String,
                                                    "TypeOfFinancing": String,
                                                    "ContractPhase": String,
                                                    "StartingDate": String,
                                                    "DateOfLastUpdate": String
                                                  }),
                                                  "EndDateOfContract": String,
                                                  "MethodOfPayment": String,
                                                  "Periodicity": String,
                                                  "CreditLimit": String,
                                                  "TypeOfInstalment": String,
                                                  "MonthlyInstalmentAmount": Float64,
                                                  "ExpirationDateofNextInstallment": String,
                                                  "ResidualAmount": String,
                                                  "MaxResidualAmount": String,
                                                  "DateOfMaximumResidualAmount": String,
                                                  "DateOfLastPayment": String,
                                                  "UnpaidDueInstalmentsNumber": Float64,
                                                  "UnpaidDueInstalmentsAmount": Float64,
                                                  "MaximumLevelOfDefault": String,
                                                  "NumberOfMonthsWithMaximumLevelOfDefault": String,
                                                  "DateOfLastCharge": String,
                                                  "AmountChargedInTheMonth": String,
                                                  "MaximumAmountChargedInTheMonth": String,
                                                  "DateOfMaximumAmountCharged": String,
                                                  "PreviousMethodOfPayment": String,
                                                  "AmountOverTheLimit": String,
                                                  "GuarantedAmountFromGuarantor": Float64,
                                                  "PersonalGuaranteeAmount": Float64,
                                                  "DateWhenOverTheLimit": String,
                                                  "ReorganizedCredit": String,
                                                  "WorstStatus": Float64,
                                                  "DateWorstStatus": String,
                                                  "MaxNrOfDaysOfPaymentDelay": String,
                                                  "DateMaxNrOfDaysOfPaymentDelay": String,
                                                  "NrOfDaysOfPaymentDelay": Float64,
                                                  "MaximumUnpaidAmount": String,
                                                  "NextDueInstalmentAmount": Float64,
                                                  "PaymentsPeriodicity": String,
                                                  "RegistrationNumber": String,
                                                  "RemainingInstalmentsAmount": Float64,
                                                  "RemainingInstalmentsNumber": Float64,
                                                  "TotalAmount": Float64,
                                                  "TotalNumberOfInstalments": Float64,
                                                  "TypeOfLeasingSubject": String,
                                                  "ValueOfLeasingSubject": String,
                                                  "YearOfManufacturing": String,
                                                  "LastPaymentDate": String,
                                                  "FlagNewUsed": String,
                                                  "Brand": String,
                                                  "AmountOfTheCredits": String,
                                                 "Profiles": List(Struct({
                                                    "ReferenceYear": String,
                                                    "ReferenceMonth": String,
                                                    "Default": String,
                                                    "Status": String,
                                                    "ResidualAmount": String,
                                                    "Utilization": String,
                                                    "GuarantedAmount": Float64,
                                                    "Granted": Float64
                                                  })),
                                                  "LinkedSubjects": String,
                                                  "CardsGuarantees": List,
                                                  "InstGuarantees": List # Sửa sang List để chứa được datatype dạng phức tạp hơn
                                                }
                                        ))



Contract_Instalments_NotGrantedContract = List(Struct({
                                                  "Amounts": Struct({
                                                    "RequestDateOfTheContract": String,
                                                    "PaymentPeriodicity": String,
                                                    "CreditLimit": String,
                                                    "MonthlyInstalmentAmount": String,
                                                    "TotalAmount": Float64,
                                                    "TotalNumberOfInstalments": Float64
                                                  }),
                                                  "ContractPhase": String,
                                                  "TypeOfFinancing": String,
                                                  "Role": String,
                                                  "ReferenceNumber": String,
                                                  "CBContractCode": String,
                                                  "EncryptedFICode": String,
                                                  "FIContractCode": String,
                                                  "LinkedSubjects": String
                                                }
                                                ))


Contract_NonInstalments_GrantedContract = List(Struct({
                                                  "CommonData": Struct({
                                                    "CBContractCode": String,
                                                    "FIContractCode": String,
                                                    "Currency": String,
                                                    "ReferenceNumber": String,
                                                    "Role": String,
                                                    "EncryptedFICode": String,
                                                    "TypeOfFinancing": String,
                                                    "ContractPhase": String,
                                                    "StartingDate": String,
                                                    "DateOfLastUpdate": String
                                                  }),
                                                  "EndDateOfContract": String,
                                                  "MethodOfPayment": String,
                                                  "Periodicity": String,
                                                  "CreditLimit": String,
                                                  "TypeOfInstalment": String,
                                                  "MonthlyInstalmentAmount": String,
                                                  "ExpirationDateofNextInstallment": String,
                                                  "ResidualAmount": String,
                                                  "MaxResidualAmount": String,
                                                  "DateOfMaximumResidualAmount": String,
                                                  "DateOfLastPayment": String,
                                                  "UnpaidDueInstalmentsNumber": String,
                                                  "UnpaidDueInstalmentsAmount": String,
                                                  "MaximumLevelOfDefault": String,
                                                  "NumberOfMonthsWithMaximumLevelOfDefault": String,
                                                  "DateOfLastCharge": String,
                                                  "AmountChargedInTheMonth": String,
                                                  "MaximumAmountChargedInTheMonth": String,
                                                  "DateOfMaximumAmountCharged": String,
                                                  "PreviousMethodOfPayment": String,
                                                  "AmountOverTheLimit": String,
                                                  "GuarantedAmountFromGuarantor": Float64,
                                                  "PersonalGuaranteeAmount": Float64,
                                                  "DateWhenOverTheLimit": String,
                                                  "ReorganizedCredit": String,
                                                  "WorstStatus": Float64,
                                                  "DateWorstStatus": String,
                                                  "MaxNrOfDaysOfPaymentDelay": Float64,
                                                  "DateMaxNrOfDaysOfPaymentDelay": String,
                                                  "NrOfDaysOfPaymentDelay": Float64,
                                                  "MaximumUnpaidAmount": String,
                                                  "NextDueInstalmentAmount": String,
                                                  "PaymentsPeriodicity": String,
                                                  "RegistrationNumber": String,
                                                  "RemainingInstalmentsAmount": String,
                                                  "RemainingInstalmentsNumber": String,
                                                  "TotalAmount": String,
                                                  "TotalNumberOfInstalments": String,
                                                  "TypeOfLeasingSubject": String,
                                                  "ValueOfLeasingSubject": String,
                                                  "YearOfManufacturing": String,
                                                  "LastPaymentDate": String,
                                                  "FlagNewUsed": String,
                                                  "Brand": String,
                                                  "AmountOfTheCredits": Float64,
                                                  "Profiles": List(Struct({
                                                    "ReferenceYear": String,
                                                    "ReferenceMonth": String,
                                                    "Default": String,
                                                    "Status": String,
                                                    "ResidualAmount": String,
                                                    "Utilization": String,
                                                    "GuarantedAmount": Float64,
                                                    "Granted": Float64
                                                  })),
                                                  "LinkedSubjects": String,
                                                  "CardsGuarantees": List,
                                                  "InstGuarantees": List
                                                }))


Contract_NonInstalments_NotGrantedContract = List(Struct({
                                                  "Amounts": Struct({
                                                    "RequestDateOfTheContract": String,
                                                    "PaymentPeriodicity": String,
                                                    "CreditLimit": String,
                                                    "MonthlyInstalmentAmount": String,
                                                    "TotalAmount": Float64,
                                                    "TotalNumberOfInstalments": String
                                                  }),
                                                  "ContractPhase": String,
                                                  "TypeOfFinancing": String,
                                                  "Role": String,
                                                  "ReferenceNumber": String,
                                                  "CBContractCode": String,
                                                  "EncryptedFICode": String,
                                                  "FIContractCode": String,
                                                  "LinkedSubjects": String
                                                }))

Contract_Cards_GrantedContract = List(Struct({
                                              "CommonData": Struct({
                                                "CBContractCode": String,
                                                "FIContractCode": String,
                                                "Currency": String,
                                                "ReferenceNumber": String,
                                                "Role": String,
                                                "EncryptedFICode": String,
                                                "TypeOfFinancing": String,
                                                "ContractPhase": String,
                                                "StartingDate": String,
                                                "DateOfLastUpdate": String
                                              }),
                                              "EndDateOfContract": String,
                                              "MethodOfPayment": String,
                                              "Periodicity": String,
                                              "CreditLimit": Float64,
                                              "TypeOfInstalment": String,
                                              "MonthlyInstalmentAmount": Float64,
                                              "ExpirationDateofNextInstallment": String,
                                              "ResidualAmount": Float64,
                                              "MaxResidualAmount": Float64,
                                              "DateOfMaximumResidualAmount": String,
                                              "DateOfLastPayment": String,
                                              "UnpaidDueInstalmentsNumber": Float64,
                                              "UnpaidDueInstalmentsAmount": Float64,
                                              "MaximumLevelOfDefault": String,
                                              "NumberOfMonthsWithMaximumLevelOfDefault": String,
                                              "DateOfLastCharge": String,
                                              "AmountChargedInTheMonth": Float64,
                                              "MaximumAmountChargedInTheMonth": String,
                                              "DateOfMaximumAmountCharged": String,
                                              "PreviousMethodOfPayment": String,
                                              "AmountOverTheLimit": String,
                                              "GuarantedAmountFromGuarantor": Float64,
                                              "PersonalGuaranteeAmount": Float64,
                                              "DateWhenOverTheLimit": String,
                                              "ReorganizedCredit": String,
                                              "WorstStatus": Float64,
                                              "DateWorstStatus": String,
                                              "MaxNrOfDaysOfPaymentDelay": String,
                                              "DateMaxNrOfDaysOfPaymentDelay": String,
                                              "NrOfDaysOfPaymentDelay": Float64,
                                              "MaximumUnpaidAmount": String,
                                              "NextDueInstalmentAmount": String,
                                              "PaymentsPeriodicity": String,
                                              "RegistrationNumber": String,
                                              "RemainingInstalmentsAmount": String,
                                              "RemainingInstalmentsNumber": String,
                                              "TotalAmount": String,
                                              "TotalNumberOfInstalments": String,
                                              "TypeOfLeasingSubject": String,
                                              "ValueOfLeasingSubject": String,
                                              "YearOfManufacturing": String,
                                              "LastPaymentDate": String,
                                              "FlagNewUsed": String,
                                              "Brand": String,
                                              "AmountOfTheCredits": String,
                                              "Profiles": List(Struct({
                                                    "ReferenceYear": String,
                                                    "ReferenceMonth": String,
                                                    "Default": String,
                                                    "Status": String,
                                                    "ResidualAmount": String,
                                                    "Utilization": String,
                                                    "GuarantedAmount": Float64,
                                                    "Granted": Float64
                                                  })),
                                              "LinkedSubjects": String,
                                              "CardsGuarantees": List,
                                              "InstGuarantees": List
                                            }
                                            ))


Contract_Cards_NotGrantedContract = List(Struct({
                                          "Amounts": Struct({
                                            "RequestDateOfTheContract": String,
                                            "PaymentPeriodicity": String,
                                            "CreditLimit": Float64,
                                            "MonthlyInstalmentAmount": String,
                                            "TotalAmount": String,
                                            "TotalNumberOfInstalments": String
                                          }),
                                          "ContractPhase": String,
                                          "TypeOfFinancing": String,
                                          "Role": String,
                                          "ReferenceNumber": String,
                                          "CBContractCode": String,
                                          "EncryptedFICode": String,
                                          "FIContractCode": String,
                                          "LinkedSubjects": String
                                        }))


CurrencySummary_CurrencyPercentage = List


Profiles = List(Struct(
                {'ReferenceYear': String, 
                 'ReferenceMonth': String, 
                 'Default': String, 
                 'Status': String, 
                 'ResidualAmount': String, 
                 'Utilization': String, 
                 'GuarantedAmount': String, 
                 'Granted': String}
            ))





