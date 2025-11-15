# presspatrion
donations data (people donate money on the hexa website).

[record_source] = 'PRESSPATRON'
# schema table
- presspatron	supporter
    # hash key
    MD5_ASCII(
    [record_source] + '|' + toString([TransactionID])
    )
- presspatron	period
    # hash key
    MD5_ASCII(
    [record_source] + '|' + toString([Frequency])
    )
- presspatron	donation
    # hash key
    MD5_ASCII(
    [record_source] + '|' + [Sign up date] + '/' + [Email address] + '/' + [Frequency] + '/' + [Amount of transaction]
    )
- presspatron	donation_csv
- presspatron	payment
    # hash key
    MD5_ASCII(
    [record_source] + '|' + toString([TransactionID]) + '/' + [Payment status]
    )

# alteryx data workflow
C:\projects\CDW_PROD\presspatron_sftp_to_stage v2.yxmd
