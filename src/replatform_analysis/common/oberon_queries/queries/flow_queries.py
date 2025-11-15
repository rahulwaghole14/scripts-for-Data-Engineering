q_1 = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-03-03T00:00:00.000/2024-03-10T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/occurrences",
                "filters": [
                    "0"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentDefinition": {
                    "container": {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "and",
                            "preds": [
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "exists",
                                        "val": {
                                            "func": "attr",
                                            "name": "variables/evar3",
                                            "allocation-model": {
                                                "func": "allocation-instance"
                                            }
                                        }
                                    }
                                },
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "sequence-prefix",
                                        "stream": [
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "other",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "section",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "static",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "static",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            }
                                        ],
                                        "context": "visits"
                                    }
                                }
                            ]
                        }
                    },
                    "func": "segment",
                    "version": [
                        1,
                        0,
                        0
                    ]
                }
            }
        ]
    },
    "dimension": "variables/evar3",
    "anchorDate": "2024-03-03T00:00:00",
    "search": {
        "itemIds": [
            "3306266643",
            "1371109424",
            "499001114",
            "903806222",
            "522641180"
        ]
    },
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": False,
        "limit": 5
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}

q_2 = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-03-03T00:00:00.000/2024-03-10T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/occurrences",
                "filters": [
                    "0"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentDefinition": {
                    "container": {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "and",
                            "preds": [
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "exists",
                                        "val": {
                                            "func": "attr",
                                            "name": "variables/evar3",
                                            "allocation-model": {
                                                "func": "allocation-instance"
                                            }
                                        }
                                    }
                                },
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "sequence-prefix",
                                        "stream": [
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "other",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "static",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "static",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "static",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            }
                                        ],
                                        "context": "visits"
                                    }
                                }
                            ]
                        }
                    },
                    "func": "segment",
                    "version": [
                        1,
                        0,
                        0
                    ]
                }
            }
        ]
    },
    "dimension": "variables/evar3",
    "anchorDate": "2024-03-03T00:00:00",
    "search": {
        "itemIds": [
            "3306266643",
            "1371109424",
            "499001114",
            "903806222",
            "522641180"
        ]
    },
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": False,
        "limit": 5
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}


q_3 = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-03-03T00:00:00.000/2024-03-10T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/occurrences",
                "filters": [
                    "0"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentDefinition": {
                    "container": {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "and",
                            "preds": [
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "exists",
                                        "val": {
                                            "func": "attr",
                                            "name": "variables/evar3",
                                            "allocation-model": {
                                                "func": "allocation-instance"
                                            }
                                        }
                                    }
                                },
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "sequence-prefix",
                                        "stream": [
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "section",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "article",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "home",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "streq",
                                                    "str": "section",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    },
                                                    "description": "Page Type"
                                                }
                                            },
                                            {
                                                "func": "dimension-restriction",
                                                "count": 1,
                                                "limit": "within",
                                                "attribute": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            }
                                        ],
                                        "context": "visits"
                                    }
                                }
                            ]
                        }
                    },
                    "func": "segment",
                    "version": [
                        1,
                        0,
                        0
                    ]
                }
            }
        ]
    },
    "dimension": "variables/evar3",
    "anchorDate": "2024-03-03T00:00:00",
    "search": {
        "itemIds": [
            "3306266643",
            "1371109424",
            "499001114",
            "903806222",
            "522641180"
        ]
    },
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": False,
        "limit": 5
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}





def q_custom(page_type_string,sys_env_segment):
    stream =    [
                    {
                        "func": "exclude-next-checkpoint"
                    },
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar3",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    }
                ] + \
                page_type_string + \
                [
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar3",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    },
                    {
                        "func": "exclude-next-checkpoint"
                    },
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar3",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    }
                ]

    output = {
            "rsid": "fairfaxnzhexa-new-replatform",
            "globalFilters": [
                {
                    "type": "segment",
                    "segmentId": "s200000657_65a05e517512d11b24e1133c"
                },
                {
                    "type": "segment",
                    "segmentId": sys_env_segment
                },
                {
                    "type": "dateRange",
                    "dateRange": "2024-03-13T00:00:00.000/2024-03-14T00:00:00.000"
                }
            ],
            "metricContainer": {
                "metrics": [
                    {
                        "columnId": "1",
                        "id": "metrics/occurrences",
                        "filters": [
                            "0"
                        ]
                    }
                ],
                "metricFilters": [
                    {
                        "id": "0",
                        "type": "segment",
                        "segmentDefinition": {
                            "container": {
                                "func": "container",
                                "context": "hits",
                                "pred": {
                                    "func": "and",
                                    "preds": [
                                        {
                                            "func": "container",
                                            "context": "hits",
                                            "pred": {
                                                "func": "exists",
                                                "val": {
                                                    "func": "attr",
                                                    "name": "variables/evar3",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "func": "container",
                                            "context": "hits",
                                            "pred": {
                                                "func": "sequence-prefix",
                                                "stream": stream,
                                                "context": "visits"
                                            }
                                        }
                                    ]
                                }
                            },
                            "func": "segment",
                            "version": [
                                1,
                                0,
                                0
                            ]
                        }
                    }
                ]
            },
            "dimension": "variables/evar3",
            "anchorDate": "2024-03-03T00:00:00",
            "settings": {
                "countRepeatInstances": True,
                "includeAnnotations": False,
            },
            "capacityMetadata": {
                "associations": [
                    {
                        "name": "applicationName",
                        "value": "Analysis Workspace UI"
                    }
                ]
            }
        }
    return output


new_flow_1 = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "dateRange",
            "dateRange": "2024-03-01T00:00:00.000/2024-04-01T00:00:00.000",
            "dateRangeId": "thisMonth"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/occurrences",
                "filters": [
                    "0"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentDefinition": {
                    "container": {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "and",
                            "preds": [
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "exists",
                                        "val": {
                                            "func": "attr",
                                            "name": "variables/evar3",
                                            "allocation-model": {
                                                "func": "allocation-instance"
                                            }
                                        }
                                    }
                                },
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "sequence-prefix",
                                        "stream": [
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            }
                                        ],
                                        "context": "visits"
                                    }
                                }
                            ]
                        }
                    },
                    "func": "segment",
                    "version": [
                        1,
                        0,
                        0
                    ]
                }
            }
        ]
    },
    "dimension": "variables/evar3",
    "anchorDate": "2024-03-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 10
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}

new_flow_2 = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "dateRange",
            "dateRange": "2024-03-01T00:00:00.000/2024-04-01T00:00:00.000",
            "dateRangeId": "thisMonth"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/occurrences",
                "filters": [
                    "0"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentDefinition": {
                    "container": {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "and",
                            "preds": [
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "exists",
                                        "val": {
                                            "func": "attr",
                                            "name": "variables/evar3",
                                            "allocation-model": {
                                                "func": "allocation-instance"
                                            }
                                        }
                                    }
                                },
                                {
                                    "func": "container",
                                    "context": "hits",
                                    "pred": {
                                        "func": "sequence-prefix",
                                        "stream": [
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "func": "exclude-next-checkpoint"
                                            },
                                            {
                                                "func": "container",
                                                "context": "hits",
                                                "pred": {
                                                    "func": "exists",
                                                    "val": {
                                                        "func": "attr",
                                                        "name": "variables/evar3",
                                                        "allocation-model": {
                                                            "func": "allocation-instance"
                                                        }
                                                    }
                                                }
                                            }
                                        ],
                                        "context": "visits"
                                    }
                                }
                            ]
                        }
                    },
                    "func": "segment",
                    "version": [
                        1,
                        0,
                        0
                    ]
                }
            }
        ]
    },
    "dimension": "variables/evar3",
    "anchorDate": "2024-03-01T00:00:00",
    "search": {
        "itemIds": [
            "3306266643",
            "1371109424",
            "499001114",
            "522641180",
            "903806222"
        ]
    },
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 10
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            }
        ]
    }
}


entry_page_q = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65f3766facc2414a5068dcb2"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-03-13T00:00:00.000/2024-03-14T00:00:00.000",
            "dateRangeId": "thisMonth"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visits",
                "sort": "desc"
            }
        ]
    },
    "dimension": "variables/entryprop3",
    "anchorDate": "2024-03-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 50,
        "page": 0,
        "nonesBehavior": "exclude-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "undefined"
            },
            {
                "name": "projectName",
                "value": "New project"
            },
            {
                "name": "panelName",
                "value": "Freeform table"
            }
        ]
    }
}


entry_page_q_old = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65f38c796ef034233393ead8"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-12-13T00:00:00.000/20234-13-14T00:00:00.000",
            "dateRangeId": "thisMonth"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visits",
                "sort": "desc"
            }
        ]
    },
    "dimension": "variables/entryprop47",
    "anchorDate": "2023-12-15T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 50,
        "page": 0,
        "nonesBehavior": "exclude-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "undefined"
            },
            {
                "name": "projectName",
                "value": "New project"
            },
            {
                "name": "panelName",
                "value": "Freeform table"
            }
        ]
    }
}






def q_custom_old(page_type_string,segment):
    stream =    [
                    {
                        "func": "exclude-next-checkpoint"
                    },
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar94",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    }
                ] + \
                page_type_string + \
                [
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar94",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    },
                    {
                        "func": "exclude-next-checkpoint"
                    },
                    {
                        "func": "container",
                        "context": "hits",
                        "pred": {
                            "func": "exists",
                            "val": {
                                "func": "attr",
                                "name": "variables/evar94",
                                "allocation-model": {
                                    "func": "allocation-instance"
                                }
                            }
                        }
                    }
                ]

    output = {
            "rsid": "fairfaxnz-hexaoverall-production",
            "globalFilters": [
                {
                    "type": "segment",
                    "segmentId": "s200000657_566f38e9e4b080432935e591"
                },
                {
                    "type": "segment",
                    "segmentId": segment
                },
                {
                    "type": "dateRange",
                    "dateRange": "2024-03-13T00:00:00.000/2024-03-14T00:00:00.000"
                }
            ],
            "metricContainer": {
                "metrics": [
                    {
                        "columnId": "1",
                        "id": "metrics/occurrences",
                        "filters": [
                            "0"
                        ]
                    }
                ],
                "metricFilters": [
                    {
                        "id": "0",
                        "type": "segment",
                        "segmentDefinition": {
                            "container": {
                                "func": "container",
                                "context": "hits",
                                "pred": {
                                    "func": "and",
                                    "preds": [
                                        {
                                            "func": "container",
                                            "context": "hits",
                                            "pred": {
                                                "func": "exists",
                                                "val": {
                                                    "func": "attr",
                                                    "name": "variables/evar94",
                                                    "allocation-model": {
                                                        "func": "allocation-instance"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "func": "container",
                                            "context": "hits",
                                            "pred": {
                                                "func": "sequence-prefix",
                                                "stream": stream,
                                                "context": "visits"
                                            }
                                        }
                                    ]
                                }
                            },
                            "func": "segment",
                            "version": [
                                1,
                                0,
                                0
                            ]
                        }
                    }
                ]
            },
            "dimension": "variables/evar94",
            "anchorDate": "2024-03-03T00:00:00",
            "settings": {
                "countRepeatInstances": True,
                "includeAnnotations": False,
            },
            "capacityMetadata": {
                "associations": [
                    {
                        "name": "applicationName",
                        "value": "Analysis Workspace UI"
                    }
                ]
            }
        }
    return output

