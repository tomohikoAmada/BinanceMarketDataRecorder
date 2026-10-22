# N2 actual isolated QA copy and pending native launch

Decision: **ACCEPT** for this completed software copy and the already reviewed exact pending QA launch.

The successful copy output7b9a16b7 has empty stderr and reports ISOLATED_QA_SOFTWARE_COPIED at UTC ns1792649341394803890. Its full manifest exactly equals the independent actual9571-bound frozen manifest, and all24 payload readback hashes match, including the serialized QA_MANIFEST07d4c135. This includes all14 runtime members, runner8308, six fixtures and the two copied data inputs. The operator54cd and runtime binding remain unchanged. Exact pending argv remains the copied/runtime pin → canonical Python runner, Typeexec/Runtime1200/Stop120/archiveTMP. Exclusive scope/attempt/result/unit guards remain as accepted.

The earlier empty stdout with Permission denied(publickey) is retained as failed authentication; it provides no copy or QA credit. It is not treated as an executed remote fixture failure.

Limits: the successful copy return exports SHA readback, not direct UID/GID/mode/directory metadata. Reviewed writer code uses root/group988 and explicit modes/fsync, but direct attribute readback is not claimed. Actual native launch, effective invocation, full fixture result and normal manager completion remain pending. No SSH, candidate execution, production or Raw operation was performed in this review, and no capture duration/T0 credit exists.
