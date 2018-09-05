package databricks

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform/helper/schema"
	db "github.com/medivo/databricks-go"
	"github.com/mitchellh/hashstructure"
)

func resourceGroups() *schema.Resource {
	return &schema.Resource{
		Create: resourceGroupsCreate,
		Read:   resourceGroupsRead,
		Update: resourceGroupsUpdate,
		Delete: resourceGroupsDelete,
		Schema: map[string]*schema.Schema{
			"groups": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Description: "Group name",
							Required:    true,
							Type:        schema.TypeString,
							ValidateFunc: func(i interface{}, s string) ([]string, []error) {
								if len(i.(string)) == 0 {
									return []string{}, []error{fmt.Errorf("group name must not be empty")}
								}
								return []string{}, []error{}
							},
						},
						"members": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Description: "User name",
										Required:    true,
										Type:        schema.TypeString,
										ValidateFunc: func(i interface{}, s string) ([]string, []error) {
											if len(i.(string)) == 0 {
												return []string{}, []error{fmt.Errorf("user name must not be empty")}
											}
											return []string{}, []error{}
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceGroupsCreate(data *schema.ResourceData, client interface{}) error {
	groups := groupsFromRD(data)
	err := createGroups(
		data,
		client.(*db.Client).Groups(),
	)
	if err != nil {
		return err
	}

	data.Set("groups", groups)
	id, err := hashstructure.Hash(groups, nil)
	if err != nil {
		return err
	}
	data.SetId(fmt.Sprintf("%d", id))

	return nil
}
func resourceGroupsRead(data *schema.ResourceData, client interface{}) error {
	ctx := context.Background()
	groups := groupsFromRD(data)
	groupsService := client.(*db.Client).Groups()

	for group := range groups {
		principalMembers, err := groupsService.Members(ctx, group)
		if err != nil {
			return err
		}
		members := make([]string, len(principalMembers))
		for i, principalMember := range principalMembers {
			members[i] = *principalMember.UserName
		}
		groups[group] = members
	}
	data.Set("groups", groups)
	id, err := hashstructure.Hash(groups, nil)
	if err != nil {
		return err
	}
	data.SetId(fmt.Sprintf("%d", id))

	return nil
}
func resourceGroupsUpdate(data *schema.ResourceData, client interface{}) error {
	// TODO(daniel): do actual diffs?
	groups := groupsFromRD(data)
	err := deleteGroups(
		client.(*db.Client).Groups(),
		groups,
	)
	if err != nil {
		return err
	}
	data.Set("groups", groups)
	id, err := hashstructure.Hash(groups, nil)
	if err != nil {
		return err
	}
	data.SetId(fmt.Sprintf("%d", id))

	return nil
}
func resourceGroupsDelete(data *schema.ResourceData, client interface{}) error {
	return deleteGroups(
		client.(*db.Client).Groups(),
		groupsFromRD(data),
	)
}

func deleteGroups(
	service *db.GroupsService,
	groups map[string][]string,
) error {
	ctx := context.Background()

	for group := range groups {
		if err := service.Delete(ctx, group); err != nil {
			return err
		}
	}

	return nil
}

func groupsFromRD(data *schema.ResourceData) map[string][]string {
	groups := map[string][]string{}

	groupConf := data.Get("groups").([]interface{})
	for _, m := range groupConf {
		groupMap := m.(map[string]interface{})
		groupName := groupMap["name"].(string)
		membersConf := groupMap["members"].([]interface{})
		members := make([]string, len(membersConf))
		for i, memberConf := range membersConf {
			innerMap := memberConf.(map[string]interface{})
			members[i] = innerMap["name"].(string)
		}

		groups[groupName] = members
	}

	return groups
}

func createGroups(
	data *schema.ResourceData,
	service *db.GroupsService,
) error {
	ctx := context.Background()
	for group, members := range groupsFromRD(data) {
		if err := service.Create(ctx, group); err != nil {
			return err
		}

		for _, member := range members {
			err := service.AddMember(
				ctx,
				member,
				"",
				group,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
