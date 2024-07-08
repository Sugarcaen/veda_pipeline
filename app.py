#!/usr/bin/env python3
import os

import aws_cdk as cdk

from veda.veda_stack import VedaStack


app = cdk.App()
VedaStack(app, "VedaStack")

app.synth()
