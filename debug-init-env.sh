#!/usr/bin/env bash
#
# Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

echo -n "Pre-creating log..."
TEMPDIR=data
touch $TEMPDIR/dd-data-vault.log
echo "OK"

echo -n "Pre-creating vault directory..."
mkdir -p $TEMPDIR/vault
mkdir -p $TEMPDIR/vault/archive
mkdir -p $TEMPDIR/vault/staging
mkdir -p $TEMPDIR/ingest/inbox
mkdir -p $TEMPDIR/ingest/outbox
echo "OK"

echo -n "Pre-creating ocfl-work directory..."
mkdir -p $TEMPDIR/ocfl-work
echo "OK"
