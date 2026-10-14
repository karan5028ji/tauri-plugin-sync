## Default Permission

Default permissions for tauri-plugin-sync: allows querying sync status, triggering immediate sync, pausing/resuming background replication, and updating safe frontend configurations.

#### This default permission set includes the following:

- `allow-sync-now`
- `allow-get-status`
- `allow-pause`
- `allow-resume`
- `allow-configure`

## Permission Table

<table>
<tr>
<th>Identifier</th>
<th>Description</th>
</tr>


<tr>
<td>

`sync:allow-configure`

</td>
<td>

Enables the configure command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:deny-configure`

</td>
<td>

Denies the configure command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:allow-get-status`

</td>
<td>

Enables the get_status command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:deny-get-status`

</td>
<td>

Denies the get_status command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:allow-pause`

</td>
<td>

Enables the pause command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:deny-pause`

</td>
<td>

Denies the pause command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:allow-resume`

</td>
<td>

Enables the resume command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:deny-resume`

</td>
<td>

Denies the resume command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:allow-sync-now`

</td>
<td>

Enables the sync_now command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`sync:deny-sync-now`

</td>
<td>

Denies the sync_now command without any pre-configured scope.

</td>
</tr>
</table>
